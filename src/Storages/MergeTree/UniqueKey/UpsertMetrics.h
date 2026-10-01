#pragma once

#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
extern const Event UpsertMaskCacheHits;
extern const Event UpsertMaskCacheMisses;
extern const Event UpsertWinnerLookupMicroseconds;
extern const Event UpsertWinnerLookupKeys;
extern const Event UpsertMaskReadMicroseconds;
extern const Event UpsertMaskReadBytes;
extern const Event UpsertMaskWriteBytes;
extern const Event UpsertStatePrepareMicroseconds;
extern const Event UpsertStateWriteMicroseconds;
extern const Event UpsertStateWriteBytes;
extern const Event UpsertStateWrites;
extern const Event UpsertCommitLockWaitMicroseconds;
extern const Event UpsertCommitLockHoldMicroseconds;
extern const Event UpsertSnapshotBuildMicroseconds;
extern const Event UpsertSnapshotPartsVisited;
extern const Event UpsertMaintenanceCandidates;
extern const Event UpsertRecoveryMicroseconds;
extern const Event UpsertRecoveryParts;
}
