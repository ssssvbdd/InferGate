package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/ssssvbdd/InferGate/services/gateway/internal/gwerr"
)

func TestParseRequestRejectsOversizedBody(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader(bytes.Repeat([]byte("x"), maxRequestBody+1)))
	_, err := parseRequest(c)
	status, _ := gwerr.HandleError(err)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d: %v", status, err)
	}
}
