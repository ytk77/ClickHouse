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
extern const Event UpsertCatalogNodesAllocated;
extern const Event UpsertCatalogVersionsMicroseconds;
extern const Event UpsertCatalogRangesMicroseconds;
extern const Event UpsertMaintenanceAdmissionDenied;
extern const Event UpsertMaintenanceAdmissionPrecheckDeferrals;
extern const Event UpsertMaintenanceOversizedAdmissions;
extern const Event UpsertMaintenanceSizeRatioSkips;
extern const Event UpsertMaintenanceMemoryBackoffs;
extern const Event UpsertMaintenanceRewritesStarted;
extern const Event UpsertMaintenanceReadBytes;
extern const Event UpsertMaintenanceWrittenBytes;
extern const Event UpsertMaintenancePeakMemoryBytes;
extern const Event UpsertMaintenanceCandidates;
extern const Event UpsertMaintenanceCorruptionFailures;
extern const Event UpsertMaintenanceFailures;
extern const Event UpsertMaintenanceSaturatedSkips;
extern const Event UpsertPartCachePrewarms;
extern const Event UpsertPartCachePrewarmFailures;
extern const Event UpsertRecoveryMicroseconds;
extern const Event UpsertRecoveryParts;
extern const Event UpsertRecoveryMetadataParts;
extern const Event UpsertPayloadVerificationParts;
extern const Event UpsertPayloadVerificationMicroseconds;
}
