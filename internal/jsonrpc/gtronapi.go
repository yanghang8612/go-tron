package jsonrpc

// GtronAPI exposes go-tron-specific operational diagnostics.
type GtronAPI struct {
	backend Backend
}

func NewGtronAPI(backend Backend) *GtronAPI {
	return &GtronAPI{backend: backend}
}

func (api *GtronAPI) FreezerStatus() (*FreezerStatus, error) {
	return api.backend.FreezerStatus()
}

func (api *GtronAPI) StageStatus() (*StageStatus, error) {
	return api.backend.StageStatus()
}
