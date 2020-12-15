package org.icgc_argo.workflowgraphnode.config;

import org.icgc_argo.workflow_graph_lib.workflow.client.oauth.ClientCredentials;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("oauth")
@Configuration
public class OAuthConfig {

  public final ClientCredentials clientCredentials;

  public OAuthConfig(
      @Value("${oauth.clientCredentials.clientId}") String clientId,
      @Value("${oauth.clientCredentials.clientSecret}") String clientSecret,
      @Value("${oauth.clientCredentials.tokenUri}") String tokenUri,
      @Value("${oauth.clientCredentials.publicKeyUri}") String publicKeyUri) {
    this.clientCredentials =
        ClientCredentials.builder()
            .clientId(clientId)
            .clientSecret(clientSecret)
            .tokenUri(tokenUri)
            .publicKeyUri(publicKeyUri)
            .build();
  }

  @Bean
  public ClientCredentials getClientCredentials() {
    return clientCredentials;
  }
}
