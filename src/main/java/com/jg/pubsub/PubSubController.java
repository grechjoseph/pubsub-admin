package com.jg.pubsub;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.ExpirationPolicy;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class PubSubController {

    private final String projectId = "jg-gcp";

    @PostMapping("/topics/{topicId}/subscriptions")
    public void createSubscription(@PathVariable final String topicId, @RequestBody final AddSubscriptionRequest request) {
        try (final SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            final PushConfig pushConfig = PushConfig.newBuilder()
                    .setPushEndpoint(request.getPushEndpoint())
                    .build();

            final Subscription subscriptionToCreate = Subscription.newBuilder()
                    .setName("projects/jg-gcp/subscriptions/" + request.getSubscriptionName())
                    .setTopic("projects/jg-gcp/topics/my-topic")
                    .setMessageRetentionDuration(Duration.newBuilder().setSeconds(604800).build())
                    .setRetainAckedMessages(true)
                    .setExpirationPolicy(ExpirationPolicy.newBuilder().setTtl(Duration.newBuilder().setSeconds(2592000).build()).build())
                    .setAckDeadlineSeconds(10)
                    .setFilter("attributes.customer-id=\"123\"")
                    .setPushConfig(pushConfig)
                    .build();

            final Subscription subscription = subscriptionAdminClient.createSubscription(subscriptionToCreate);
            log.info("Subscription [{}] created successfully.", subscription.getName());
        } catch (final Exception ex) {
            log.error("Failed to create subscriptions: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AddSubscriptionRequest {

        private String subscriptionName;
        private String pushEndpoint;

    }


}
