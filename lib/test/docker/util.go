package docker

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
)

func GetHostIp(ctx context.Context) (string, error) {
	provider, err := testcontainers.NewDockerProvider(testcontainers.DefaultNetwork("bridge"))
	if err != nil {
		return "", err
	}
	return provider.GetGatewayIP(ctx)
}
