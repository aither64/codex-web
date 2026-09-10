package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStandaloneCapabilitiesCoverMountedConversation(t *testing.T) {
	capabilities := standaloneCapabilities()
	if !capabilities.Read || !capabilities.Pending || !capabilities.QueueRead ||
		!capabilities.Send || !capabilities.Queue || !capabilities.Interrupt ||
		!capabilities.Settings || !capabilities.Respond || !capabilities.EventStream {
		t.Fatalf("standalone example must grant every capability used by the mounted conversation: %+v", capabilities)
	}
}

func TestStandaloneExampleRequiresLoopbackEndpoints(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080"} {
		if err := requireLoopbackAddress(address); err != nil {
			t.Errorf("loopback address %q: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:8080", "[::]:8080", ":8080", "example.test:8080"} {
		if err := requireLoopbackAddress(address); err == nil {
			t.Errorf("non-loopback address %q was accepted", address)
		}
	}
	for _, origin := range []string{"http://127.0.0.1:8080", "http://[::1]:8080", "http://localhost:8080"} {
		if err := requireLoopbackOrigin(origin); err != nil {
			t.Errorf("loopback origin %q: %v", origin, err)
		}
	}
	for _, origin := range []string{"https://example.test", "http://0.0.0.0:8080", "http://localhost:8080/path"} {
		if err := requireLoopbackOrigin(origin); err == nil {
			t.Errorf("non-loopback origin %q was accepted", origin)
		}
	}
}

func TestStandaloneExampleSetsAntiFramingHeaders(t *testing.T) {
	handler := securityHeaders(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Header().Get("X-Frame-Options") != "DENY" ||
		response.Header().Get("X-Content-Type-Options") != "nosniff" ||
		response.Header().Get("Content-Security-Policy") !=
			"default-src 'self'; connect-src 'self'; frame-ancestors 'none'" {
		t.Fatalf("security headers = %v", response.Header())
	}
}
