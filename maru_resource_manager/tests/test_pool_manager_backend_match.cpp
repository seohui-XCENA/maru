// Copyright 2026 XCENA Inc.
// Unit tests for PoolManager::backendMatchesPool — the BackendTag ↔ DaxType
// policy used by alloc() to route requests to the right pool kind.

#include <gtest/gtest.h>

#include "pool_manager.h"

using namespace maru;

TEST(PoolManagerBackendMatch, UnspecifiedAcceptsAllTypes) {
    EXPECT_TRUE(PoolManager::backendMatchesPool(BackendTag::UNSPECIFIED, DaxType::DEV_DAX));
    EXPECT_TRUE(PoolManager::backendMatchesPool(BackendTag::UNSPECIFIED, DaxType::FS_DAX));
    EXPECT_TRUE(PoolManager::backendMatchesPool(BackendTag::UNSPECIFIED, DaxType::MARUFS));
}

TEST(PoolManagerBackendMatch, MaruTagCoversDevDaxAndFsDax) {
    EXPECT_TRUE(PoolManager::backendMatchesPool(BackendTag::MARU, DaxType::DEV_DAX));
    EXPECT_TRUE(PoolManager::backendMatchesPool(BackendTag::MARU, DaxType::FS_DAX));
    EXPECT_FALSE(PoolManager::backendMatchesPool(BackendTag::MARU, DaxType::MARUFS));
}

TEST(PoolManagerBackendMatch, MarufsTagOnlyMatchesMarufs) {
    EXPECT_FALSE(PoolManager::backendMatchesPool(BackendTag::MARUFS, DaxType::DEV_DAX));
    EXPECT_FALSE(PoolManager::backendMatchesPool(BackendTag::MARUFS, DaxType::FS_DAX));
    EXPECT_TRUE(PoolManager::backendMatchesPool(BackendTag::MARUFS, DaxType::MARUFS));
}
