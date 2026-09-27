#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "market_data/event_bus.hpp"

namespace market_data {
namespace {

TEST(BoundedQueueTest, PushAndPopPreserveOrder) {
    BoundedQueue<int> queue(8);
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(queue.push(i));
    }
    EXPECT_EQ(queue.size(), 5u);
    for (int i = 0; i < 5; ++i) {
        const auto value = queue.pop();
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, i);
    }
}

TEST(BoundedQueueTest, HighWaterMarkTracksPeakNotCurrentDepth) {
    BoundedQueue<int> queue(16);
    for (int i = 0; i < 9; ++i) {
        queue.push(i);
    }
    for (int i = 0; i < 9; ++i) {
        queue.pop();
    }
    EXPECT_EQ(queue.size(), 0u);
    EXPECT_EQ(queue.high_water_mark(), 9u);
}

TEST(BoundedQueueTest, DropNewestDiscardsAndCountsWhenFull) {
    BoundedQueue<int> queue(2, OverflowPolicy::DropNewest);
    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));

    EXPECT_FALSE(queue.push(3));
    EXPECT_FALSE(queue.push(4));
    EXPECT_EQ(queue.dropped_count(), 2u);

    // The events already queued are untouched: it is the newest that is lost.
    EXPECT_EQ(*queue.pop(), 1);
    EXPECT_EQ(*queue.pop(), 2);
}

TEST(BoundedQueueTest, BlockPolicyNeverDropsEvenUnderSustainedOverload) {
    // This is the guarantee the default configuration makes: a slow consumer
    // costs latency, never data.
    constexpr int kTotal = 5000;
    BoundedQueue<int> queue(16, OverflowPolicy::Block);

    std::thread producer([&queue] {
        for (int i = 0; i < kTotal; ++i) {
            queue.push(i);
        }
        queue.close();
    });

    int received = 0;
    int expected = 0;
    bool ordered = true;
    while (const auto value = queue.pop()) {
        ordered = ordered && (*value == expected++);
        ++received;
    }
    producer.join();

    EXPECT_EQ(received, kTotal);
    EXPECT_TRUE(ordered);
    EXPECT_EQ(queue.dropped_count(), 0u);
}

TEST(BoundedQueueTest, BlockedProducerIsObservable) {
    BoundedQueue<int> queue(1, OverflowPolicy::Block);
    ASSERT_TRUE(queue.push(1));

    std::atomic<bool> pushed{false};
    std::thread producer([&] {
        queue.push(2);
        pushed.store(true);
    });

    // The producer must still be parked, since nothing has been consumed.
    while (queue.blocked_producers() == 0) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(pushed.load());

    EXPECT_EQ(*queue.pop(), 1);
    producer.join();
    EXPECT_TRUE(pushed.load());
    EXPECT_EQ(*queue.pop(), 2);
}

TEST(BoundedQueueTest, CloseDrainsRatherThanDiscards) {
    BoundedQueue<int> queue(8);
    queue.push(1);
    queue.push(2);
    queue.push(3);
    queue.close();

    // Shutdown must not throw away events already accepted.
    EXPECT_EQ(*queue.pop(), 1);
    EXPECT_EQ(*queue.pop(), 2);
    EXPECT_EQ(*queue.pop(), 3);
    EXPECT_FALSE(queue.pop().has_value());
    EXPECT_TRUE(queue.closed());
}

TEST(BoundedQueueTest, PushAfterCloseIsRejected) {
    BoundedQueue<int> queue(8);
    queue.close();
    EXPECT_FALSE(queue.push(1));
    EXPECT_EQ(queue.size(), 0u);
}

TEST(BoundedQueueTest, PopBatchDrainsUpToTheLimit) {
    BoundedQueue<int> queue(64);
    for (int i = 0; i < 10; ++i) {
        queue.push(i);
    }

    std::vector<int> batch;
    EXPECT_EQ(queue.pop_batch(batch, 4), 4u);
    EXPECT_EQ(batch, (std::vector<int>{0, 1, 2, 3}));

    EXPECT_EQ(queue.pop_batch(batch, 100), 6u);
    EXPECT_EQ(batch.size(), 6u);
    EXPECT_EQ(batch.front(), 4);
}

TEST(BoundedQueueTest, PopBatchReturnsZeroOnlyWhenClosedAndDrained) {
    BoundedQueue<int> queue(8);
    queue.push(1);
    queue.close();

    std::vector<int> batch;
    EXPECT_EQ(queue.pop_batch(batch, 16), 1u);
    EXPECT_EQ(queue.pop_batch(batch, 16), 0u);
}

TEST(BoundedQueueTest, PopBatchForDistinguishesTimeoutFromClosed) {
    BoundedQueue<int> queue(8);
    std::vector<int> batch;

    // Open and empty: a timeout, reported as zero items rather than as the
    // end of the stream. A consumer relies on this to run periodic work.
    const auto timed_out = queue.pop_batch_for(batch, 4, std::chrono::milliseconds{10});
    ASSERT_TRUE(timed_out.has_value());
    EXPECT_EQ(*timed_out, 0u);

    queue.push(1);
    queue.push(2);
    const auto got = queue.pop_batch_for(batch, 4, std::chrono::milliseconds{10});
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 2u);

    // Closed but not yet drained: still returns the remainder.
    queue.push(3);
    queue.close();
    const auto remainder = queue.pop_batch_for(batch, 4, std::chrono::milliseconds{10});
    ASSERT_TRUE(remainder.has_value());
    EXPECT_EQ(*remainder, 1u);

    // Closed and drained: end of stream.
    EXPECT_FALSE(queue.pop_batch_for(batch, 4, std::chrono::milliseconds{10}).has_value());
}

TEST(BoundedQueueTest, ZeroCapacityIsClampedRatherThanDeadlocking) {
    BoundedQueue<int> queue(0);
    EXPECT_EQ(queue.capacity(), 1u);
    EXPECT_TRUE(queue.push(1));
    EXPECT_EQ(*queue.pop(), 1);
}

TEST(BoundedQueueTest, MoveOnlyPayloadsAreSupported) {
    BoundedQueue<std::unique_ptr<int>> queue(4);
    ASSERT_TRUE(queue.push(std::make_unique<int>(42)));
    const auto value = queue.pop();
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(*value, nullptr);
    EXPECT_EQ(**value, 42);
}

TEST(BoundedQueueTest, ManyProducersAndOneConsumerLoseNothing) {
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 2000;
    BoundedQueue<int> queue(32, OverflowPolicy::Block);

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&queue] {
            for (int i = 0; i < kPerProducer; ++i) {
                queue.push(i);
            }
        });
    }

    std::atomic<int> received{0};
    std::thread consumer([&] {
        std::vector<int> batch;
        while (queue.pop_batch(batch, 128) > 0) {
            received.fetch_add(static_cast<int>(batch.size()));
        }
    });

    for (auto& producer : producers) {
        producer.join();
    }
    queue.close();
    consumer.join();

    EXPECT_EQ(received.load(), kProducers * kPerProducer);
    EXPECT_EQ(queue.dropped_count(), 0u);
}

}  // namespace
}  // namespace market_data
