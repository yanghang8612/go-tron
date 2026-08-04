package snapshots

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type HTTPServerConfig struct {
	Addr string
	Dir  string
}

// HTTPServer serves only a signed catalog and files leased by immutable
// published manifests. It deliberately does not expose directory listings,
// mutable build files, temporary ETL files, or retired unleased segments.
// net/http's ServeContent path provides exact HTTP Range support used by the
// resumable bootstrap downloader.
type HTTPServer struct {
	cfg        HTTPServerConfig
	httpServer *http.Server
	listener   net.Listener

	registryMu sync.Mutex
	registry   map[string]string
	catalogMod fileModIdentity
	publishMod fileModIdentity
}

type fileModIdentity struct {
	exists bool
	size   int64
	mtime  int64
}

func NewHTTPServer(cfg HTTPServerConfig) *HTTPServer {
	s := &HTTPServer{cfg: cfg}
	s.httpServer = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *HTTPServer) ListenAddr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *HTTPServer) Start() error {
	if s == nil {
		return nil
	}
	if strings.TrimSpace(s.cfg.Dir) == "" {
		return errors.New("snapshots: HTTP server directory is required")
	}
	if _, err := s.refreshRegistry(true); err != nil {
		return fmt.Errorf("snapshots: initialize HTTP publication registry: %w", err)
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("snapshot HTTP listen: %w", err)
	}
	s.listener = ln
	coldSnapshotLog.Info("Signed snapshot HTTP server listening", "addr", ln.Addr().String(), "dir", s.cfg.Dir)
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			coldSnapshotLog.Error("Signed snapshot HTTP server stopped", "err", err)
		}
	}()
	return nil
}

func (s *HTTPServer) Stop() error {
	if s == nil || s.httpServer == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rel := strings.TrimPrefix(r.URL.Path, "/")
	if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || hasParentDir(rel) {
		http.NotFound(w, r)
		return
	}
	if rel == SnapshotCatalogFile {
		s.servePublishedFile(w, r, rel, "", false)
		return
	}
	registry, err := s.refreshRegistry(false)
	if err != nil {
		http.Error(w, "snapshot publication unavailable", http.StatusServiceUnavailable)
		return
	}
	checksum, ok := registry[rel]
	if !ok {
		// A catalog may have switched between the cheap metadata check and this
		// lookup. Force one refresh before returning a stable 404.
		registry, err = s.refreshRegistry(true)
		if err != nil {
			http.Error(w, "snapshot publication unavailable", http.StatusServiceUnavailable)
			return
		}
		checksum, ok = registry[rel]
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.servePublishedFile(w, r, rel, checksum, true)
}

func (s *HTTPServer) servePublishedFile(w http.ResponseWriter, r *http.Request, rel, checksum string, immutable bool) {
	abs := filepath.Join(s.cfg.Dir, rel)
	info, err := os.Stat(abs)
	if err != nil || !info.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	if immutable {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		if digest, ok := snapshotChecksumDigest(checksum); ok {
			w.Header().Set("ETag", `"`+digest+`"`)
		}
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeFile(w, r, abs)
}

func (s *HTTPServer) refreshRegistry(force bool) (map[string]string, error) {
	s.registryMu.Lock()
	defer s.registryMu.Unlock()
	catalogMod, err := snapshotFileModIdentity(filepath.Join(s.cfg.Dir, SnapshotCatalogFile))
	if err != nil {
		return nil, err
	}
	publishMod, err := snapshotFileModIdentity(filepath.Join(s.cfg.Dir, SnapshotPublishedDir))
	if err != nil {
		return nil, err
	}
	if !force && s.registry != nil && catalogMod == s.catalogMod && publishMod == s.publishMod {
		return s.registry, nil
	}
	registry, err := loadPublishedHTTPRegistry(s.cfg.Dir, catalogMod.exists)
	if err != nil {
		return nil, err
	}
	s.registry = registry
	s.catalogMod = catalogMod
	s.publishMod = publishMod
	return s.registry, nil
}

func loadPublishedHTTPRegistry(dir string, hasCatalog bool) (map[string]string, error) {
	registry := make(map[string]string)
	if !hasCatalog {
		return registry, nil
	}
	catalog, err := LoadSnapshotCatalog(dir)
	if err != nil {
		return nil, err
	}
	published, err := LoadPublishedSnapshotManifests(dir)
	if err != nil {
		return nil, err
	}
	currentFound := false
	for _, generation := range published {
		manifestChecksum, err := checksumFile(filepath.Join(dir, generation.Path))
		if err != nil {
			return nil, err
		}
		if err := registerPublishedHTTPPath(registry, generation.Path, manifestChecksum); err != nil {
			return nil, err
		}
		if generation.Path == catalog.ManifestPath {
			currentFound = strings.EqualFold(manifestChecksum, catalog.ManifestChecksum)
		}
		for _, ref := range generation.Manifest.Segments {
			if err := registerPublishedHTTPPath(registry, ref.Path, ref.Checksum); err != nil {
				return nil, err
			}
		}
	}
	if catalog.ManifestPath == ManifestFile {
		// Legacy catalog compatibility is read-only and limited to the exact
		// signed root manifest plus its active segment set.
		data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(checksumBytes(data), catalog.ManifestChecksum) {
			return nil, errors.New("snapshots: legacy catalog manifest checksum mismatch")
		}
		manifest, err := decodeProductionManifest(data)
		if err != nil {
			return nil, err
		}
		if err := registerPublishedHTTPPath(registry, ManifestFile, catalog.ManifestChecksum); err != nil {
			return nil, err
		}
		for _, ref := range manifest.Segments {
			if err := registerPublishedHTTPPath(registry, ref.Path, ref.Checksum); err != nil {
				return nil, err
			}
		}
		currentFound = true
	}
	if !currentFound {
		return nil, fmt.Errorf("snapshots: current catalog manifest %q is not a complete published generation", catalog.ManifestPath)
	}
	return registry, nil
}

func registerPublishedHTTPPath(registry map[string]string, path, checksum string) error {
	if path == "" || checksum == "" {
		return fmt.Errorf("snapshots: published HTTP path %q is missing checksum", path)
	}
	if existing, ok := registry[path]; ok && !strings.EqualFold(existing, checksum) {
		return fmt.Errorf("snapshots: published HTTP path %q has conflicting checksums", path)
	}
	registry[path] = checksum
	return nil
}

func snapshotFileModIdentity(path string) (fileModIdentity, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fileModIdentity{}, nil
	}
	if err != nil {
		return fileModIdentity{}, err
	}
	return fileModIdentity{exists: true, size: info.Size(), mtime: info.ModTime().UnixNano()}, nil
}
