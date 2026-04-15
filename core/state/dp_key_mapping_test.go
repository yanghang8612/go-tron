package state

import "testing"

func TestJavaGetterMap_NoDuplicateTargets(t *testing.T) {
	seen := make(map[string]string, len(javaGetterToGoKeyMap))
	for getter, goKey := range javaGetterToGoKeyMap {
		if other, dup := seen[goKey]; dup {
			t.Errorf("go key %q is mapped from both %q and %q", goKey, other, getter)
		}
		seen[goKey] = getter
	}
}

func TestJavaGetterMap_NoEmptyTargets(t *testing.T) {
	for getter, goKey := range javaGetterToGoKeyMap {
		if goKey == "" {
			t.Errorf("java getter %q maps to empty string", getter)
		}
	}
}

func TestJavaGetterMap_GettersUseGetPrefix(t *testing.T) {
	for getter := range javaGetterToGoKeyMap {
		if len(getter) < 4 || getter[:3] != "get" {
			t.Errorf("java key %q does not start with 'get'", getter)
		}
	}
}

func TestJavaGetterMap_GoKeysAreSnakeCase(t *testing.T) {
	for _, goKey := range javaGetterToGoKeyMap {
		for i, r := range goKey {
			switch {
			case r >= 'a' && r <= 'z':
			case r >= '0' && r <= '9':
			case r == '_':
			default:
				t.Errorf("go key %q has non-snake character %q at index %d", goKey, r, i)
				break
			}
		}
	}
}
