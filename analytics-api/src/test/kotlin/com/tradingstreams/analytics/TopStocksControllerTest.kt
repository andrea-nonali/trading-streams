package com.tradingstreams.analytics

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.core.ParameterizedTypeReference
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.http.HttpMethod
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class TopStocksControllerTest {

    companion object {
        @Container
        @JvmStatic
        val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("redis:7.2"))
            .withExposedPorts(6379)

        @DynamicPropertySource
        @JvmStatic
        fun redisProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.redis.host") { redis.host }
            registry.add("spring.data.redis.port") { redis.getMappedPort(6379) }
        }
    }

    @Autowired
    private lateinit var restTemplate: TestRestTemplate

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @BeforeEach
    fun seed() {
        redisTemplate.opsForZSet().apply {
            add("leaderboard", "AAPL", 1500.0)
            add("leaderboard", "TSLA", 1200.0)
            add("leaderboard", "NVDA", 900.0)
            add("leaderboard", "MSFT", 700.0)
            add("leaderboard", "GOOGL", 500.0)
            add("leaderboard", "AMZN", 300.0)
        }
    }

    @AfterEach
    fun cleanup() {
        redisTemplate.delete("leaderboard")
    }

    @Test
    fun `returns top 5 stocks ordered by tick count`() {
        val response = restTemplate.exchange("/api/top-stocks", HttpMethod.GET, null, object : ParameterizedTypeReference<List<Map<String, Any>>>() {}).body

        assertThat(response).hasSize(5)
        assertThat(response!![0]["symbol"]).isEqualTo("AAPL")
        assertThat(response[4]["symbol"]).isEqualTo("GOOGL")
    }

    @Test
    fun `returns empty list when leaderboard is empty`() {
        redisTemplate.delete("leaderboard")
        val response = restTemplate.exchange("/api/top-stocks", HttpMethod.GET, null, object : ParameterizedTypeReference<List<Map<String, Any>>>() {}).body
        assertThat(response).isEmpty()
    }

    @Test
    fun `returns exactly 5 stocks even when leaderboard has more`() {
        val response = restTemplate.exchange("/api/top-stocks", HttpMethod.GET, null, object : ParameterizedTypeReference<List<Map<String, Any>>>() {}).body
        assertThat(response).hasSize(5)
    }
}
