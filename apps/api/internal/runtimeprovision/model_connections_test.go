package runtimeprovision

import "testing"

func TestUserEndpointEnvironment(t *testing.T) {
	c := testActivationConfig()
	c.ModelPreset = "openai-chat"
	c.ModelID = "custom-model"
	c.ModelBaseURL = "https://api.example/v1"
	c.ModelPublicEndpoint = true
	c.ProviderAPIKey = ""
	c.APIConnectionID = "connection"
	c.APIConnectionVersion = "version"
	c.APIConnectionHumanID = "human"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	env := activationEnvironment(c)
	if env["SUMI_MODEL_BASE_URL"] != c.ModelBaseURL || env["SUMI_MODEL_PUBLIC_ENDPOINT"] != "true" || env["SUMI_PROVIDER_API_KEY"] != "" || env["SUMI_API_CONNECTION_ID"] != "connection" || env["SUMI_API_CONNECTION_VERSION"] != "version" || env["SUMI_API_CONNECTION_HUMAN_ID"] != "human" {
		t.Fatal("endpoint boundary lost")
	}
	c.ModelBaseURL = "http://localhost"
	if c.Validate() == nil {
		t.Fatal("public endpoint allowed HTTP")
	}
}
