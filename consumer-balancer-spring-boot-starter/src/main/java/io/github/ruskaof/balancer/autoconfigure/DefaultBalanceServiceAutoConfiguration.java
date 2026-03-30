package io.github.ruskaof.balancer.autoconfigure;

import io.github.ruskaof.balancer.balance.BalanceService;
import io.github.ruskaof.balancer.balance.SortingRoundRobinBalanceService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnProperty(name = "consumer-balancer.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnMissingBean(BalanceService.class)
public class DefaultBalanceServiceAutoConfiguration {

    @Bean
    public BalanceService balanceService() {
        return new SortingRoundRobinBalanceService();
    }
}
