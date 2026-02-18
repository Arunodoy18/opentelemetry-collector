# Issue #14174 Analysis: Non-Retryable HTTP Responses Persisted to Queue

## Executive Summary

**BUG CONFIRMED**: Non-retryable HTTP responses (400, 401, 403, 404, 500, etc.) ARE incorrectly persisted to the file storage queue despite being classified as permanent errors that should be dropped immediately.

**Root Cause**: Architectural ordering issue - the queue persists items to storage BEFORE the retry sender checks if errors are permanent.

**Impact**: Production systems using persistent queues (file_storage extension) will accumulate non-retryable failed requests in storage, wasting disk space and potentially replaying them on restart.

---

## 1. Root Cause Analysis

### Sender Chain Architecture

The exporter helper constructs a chain of senders in the following order (see [base_exporter.go](exporter/exporterhelper/internal/base_exporter.go#L57-L104)):

```
QueueSender → ObsReportSender → RetrySender → TimeoutSender → BaseExporter
```

### Execution Flow for Persistent Queue

When a request is sent through the system:

1. **QueueBatch.Send()** is called ([queue_batch.go](exporter/exporterhelper/internal/queuebatch/queue_batch.go#L93))
2. **queue.Offer()** is called immediately ([queue_batch.go](exporter/exporterhelper/internal/queuebatch/queue_batch.go#L96))
3. **For persistent queues**, `Offer()` calls `putInternal()` which **WRITES TO DISK IMMEDIATELY** ([persistent_queue.go](exporter/exporterhelper/internal/queue/persistent_queue.go#L293-L320))
   ```go
   ops := []*storage.Operation{
       storage.SetOperation(metadataKey, metadataBuf),
       storage.SetOperation(getItemKey(pq.metadata.WriteIndex-1), reqBuf),
   }
   if err := pq.client.Batch(ctx, ops...); err != nil {
       return err
   }
   ```
4. Queue consumers pick up the item and call the batcher
5. Batcher calls the exportFunc which calls ObsReportSender
6. ObsReportSender calls RetrySender
7. **ONLY NOW** does RetrySender check `consumererror.IsPermanent(err)` ([retry_sender.go](exporter/exporterhelper/internal/retry_sender.go#L97))
8. If permanent, the request is dropped with error "not retryable error"

**The problem**: The item was already persisted to disk in step 3, but the permanent error check doesn't happen until step 7.

### Code Path Diagram

```
User Request
    ↓
QueueBatch.Send() ← Entry point for queue
    ↓
queue.Offer(ctx, req)
    ↓
persistentQueue.Offer() ← Checks capacity
    ↓
persistentQueue.putInternal() ← CRITICAL: WRITES TO DISK HERE
    ↓
pq.client.Batch(ctx, ops...) ← File storage write happens
    ↓
[Item is now persisted to disk]
    ↓
Queue Consumer goroutine picks up item
    ↓
Batcher.Consume()
    ↓
exportFunc() (from queue_sender.go)
    ↓
ObsReportSender.Send()
    ↓
RetrySender.Send() ← Retry loop starts
    ↓
next.Send() → Returns error
    ↓
if consumererror.IsPermanent(err) { ← CHECKING TOO LATE
    return fmt.Errorf("not retryable error: %w", err)
}
    ↓
Item dropped, but ALREADY PERSISTED above!
```

---

## 2. Error Classification Analysis

### Retryable Status Codes (from [otlp.go](exporter/otlphttpexporter/otlp.go#L256-L269))

```go
func isRetryableStatusCode(code int) bool {
    switch code {
    case http.StatusTooManyRequests:        // 429
        return true
    case http.StatusBadGateway:            // 502
        return true
    case http.StatusServiceUnavailable:    // 503
        return true
    case http.StatusGatewayTimeout:        // 504
        return true
    default:
        return false
    }
}
```

### Non-Retryable Status Codes (Marked as Permanent)

All other HTTP codes are non-retryable and wrapped with `consumererror.NewPermanent()`:
- 400 Bad Request
- 401 Unauthorized
- 403 Forbidden
- 404 Not Found
- 500 Internal Server Error
- etc.

These should **NEVER** be queued or persisted, but **currently they are**.

---

## 3. Reproduction Steps

### Minimal Configuration

```yaml
receivers:
  kafka:
    # ... kafka config

processors:
  batch:
    # ... batch config

extensions:
  file_storage:
    directory: /var/lib/otelcol/storage

exporters:
  otlphttp:
    endpoint: https://backend.example.com
    sending_queue:
      enabled: true
      storage: file_storage  # ← Persistent queue enabled

service:
  extensions: [file_storage]
  pipelines:
    metrics:
      receivers: [kafka]
      processors: [batch]
      exporters: [otlphttp]
```

### Steps to Reproduce

1. Configure collector with persistent queue (file_storage)
2. Send metrics to a backend that returns HTTP 400 (Bad Request)
3. Observe that the queue storage directory contains persisted items
4. Even though the error is non-retryable, items remain in storage
5. On collector restart, these items will be re-sent (and fail again)

---

## 4. Impact Assessment

### Severity: **HIGH**

**Affected Systems:**
- Any collector using OTLP HTTP exporter (or similar) with persistent queue enabled
- Production environments with misconfigured backends or auth issues

**Consequences:**
1. **Disk Space Waste**: Non-retryable errors accumulate in storage indefinitely
2. **Performance Degradation**: Queue workers waste CPU retrying permanent failures
3. **Replay on Restart**: Persisted permanent failures are re-sent after collector restart
4. **Misleading Metrics**: Queue size metrics don't accurately reflect retryable data
5. **Debugging Difficulty**: Users see data in queue storage but it never succeeds

### Example Production Scenario

**Scenario**: Backend authentication token expires

1. Backend starts returning 401 Unauthorized (non-retryable)
   2. All incoming requests are queued to persistent storage  
3. Storage fills with thousands of non-retryable requests
4. Admin fixes auth configuration
5. **Problem**: Old 401-failed requests are still in storage and get replayed
6. Admin must manually clear storage directory

---

## 5. Proposed Fix

### Option 1: Move Retry Sender Before Queue Sender (BREAKING)

**Approach**: Reorder the sender chain so permanent errors are detected before queueing.

```go
// NEW ORDER:
// RetrySender → QueueSender → ObsReportSender → TimeoutSender → BaseExporter
```

**Pros**:
- Clean architectural fix
- Permanent errors never reach the queue
- Matches expected behavior

**Cons**:
- **BREAKING CHANGE**: Changes retry semantics
- Retries happen BEFORE queue, which may not be desired
- Could impact performance (retries block before queueing)

**Verdict**: ❌ **NOT RECOMMENDED** - Too invasive, changes fundamental behavior

---

### Option 2: Pass Error Classification to Queue (RECOMMENDED)

**Approach**: Modify the queue consumer to check for permanent errors before persisting.

**Implementation**:

1. Add a pre-check function to the queue consumer that validates if an item should be queued
2. Modify `QueueBatch.Send()` to perform a "dry run" of the export to check error type
3. Only persist to storage if the error is retryable or there is no error

**Pseudo-code**:

```go
// In queue_batch.go, modify Send():
func (qs *QueueBatch) Send(ctx context.Context, req request.Request) error {
    // For persistent queues with retry enabled, check error type first
    if qs.isPersistentQueue() && qs.hasRetrySender() {
        // Do a trial send to check error type
        err := qs.trySend(ctx, req)
        if err != nil && consumererror.IsPermanent(err) {
            // Don't queue permanent errors
            return err
        }
    }
    
    // Proceed with normal queueing
    return qs.queue.Offer(ctx, req)
}
```

**Pros**:
- Minimal code changes
- No breaking changes to semantics
- Fixes the specific issue
- Backward compatible

**Cons**:
- Requires passing context about sender chain to queue
- May need architectural refactoring to access retry logic from queue layer
- Potential performance hit (extra send attempt)

**Verdict**: ⚠️ **POSSIBLE** but needs careful design

---

### Option 3: Defer Persistence Until After First Success (RECOMMENDED)

**Approach**: Don't persist items immediately in `Offer()`. Instead, persist only after the first send attempt fails with a RETRYABLE error.

**Implementation**:

1. Modify `persistentQueue.Offer()` to add items to an in-memory staging area
2. Queue consumers attempt to send from staging area
3. Only write to persistent storage if the error is retryable
4. Delete from storage if the send succeeds or error is permanent

**Pseudo-code**:

```go
// In persistent_queue.go:
func (pq *persistentQueue[T]) Offer(ctx context.Context, req T) error {
    // Add to in-memory queue only
    pq.memoryQueue.Offer(ctx, req)
}

// In queue consumer (new wrapper around existing consume):
func (pq *persistentQueue[T]) consumeWithPersistence(ctx context.Context, req T, done Done) {
    err := pq.originalConsume(ctx, req, done)
    
    if err == nil {
        // Success - no persistence needed
        return
    }
    
    if consumererror.IsPermanent(err) {
        // Permanent error - don't persist
        done.OnDone(err)
        return
    }
    
    // Retryable error - NOW persist to storage
    pq.persistToStorage(ctx, req)
    done.OnDone(err)
}
```

**Pros**:
- Clean separation of concerns
- No changes to retry logic
- No breaking changes
- Items only persisted if actually retryable

**Cons**:
- More complex queue implementation
- Requires hybrid memory+persistent queue
- Risk of data loss if collector crashes before persistence
- Changes queue capacity semantics

**Verdict**: ⚠️ **RISKY** - Could lose data on crash

---

### Option 4: Add Permanent Error Filter to Queue Consumer (SIMPLE FIX)

**Approach**: Add a check in the queue consumer's export function to drop permanent errors without persisting.

**Implementation**:

Modify the exportFunc in [queue_sender.go](exporter/exporterhelper/internal/queue_sender.go#L44-L53):

```go
func NewQueueSender(...) (sender.Sender[request.Request], error) {
    exportFunc := func(ctx context.Context, req request.Request) error {
        itemsCount := req.ItemsCount()
        if errSend := next.Send(ctx, req); errSend != nil {
            // NEW: Check if error is permanent
            if consumererror.IsPermanent(errSend) {
                qSet.Telemetry.Logger.Info("Dropping request with permanent error",
                    zap.Error(errSend), zap.Int("dropped_items", itemsCount))
                // Return nil to mark as "successfully processed" so it's removed from queue
                return nil
            }
            
            qSet.Telemetry.Logger.Error("Exporting failed. Dropping data."+exportFailureMessage,
                zap.Error(errSend), zap.Int("dropped_items", itemsCount))
            return errSend
        }
        return nil
    }

    return queuebatch.NewQueueBatch(qSet, qCfg, exportFunc)
}
```

**Wait, this won't work!** The item is already persisted before this function is called!

**Verdict**: ❌ **DOESN'T FIX THE ISSUE**

---

### Option 5: Add Persistent Queue Awareness to Retry Sender (HYBRID APPROACH)

**Approach**: Make the retry sender aware of whether a persistent queue is downstream, and handle permanent errors differently.

This is similar to Option 2 but inverted - instead of queue checking retry logic, retry sender checks if queue is persistent.

**Verdict**: ❌ **TOO COMPLEX** - Cross-layer coupling

---

## 6. RECOMMENDED SOLUTION

After analyzing all options, here's the recommended approach:

### Two-Phase Fix

#### Phase 1: Immediate Mitigation (Documentation)

Update documentation to warn users:

```markdown
**IMPORTANT**: When using persistent queues (e.g., file_storage extension),
non-retryable errors (HTTP 400, 401, 403, 404, 500, etc.) will be persisted
to storage despite being permanent failures. This can waste disk space.

**Workaround**: Ensure your backend configuration is correct before enabling
persistent queues. Monitor storage directory size and clear manually if needed.
```

#### Phase 2: Architectural Fix (Code Change)

**Proposal**: Modify persistent queue to support "lazy persistence" mode.

Add a new queue setting:

```go
type Config struct {
    // ... existing fields ...
    
    // OnlyPersistRetryable, when true, only persists items to storage after
    // the first send attempt fails with a retryable error. This prevents
    // non-retryable errors from wasting persistent storage space.
    //
    // Default: false (persist all items immediately for backward compatibility)
    OnlyPersistRetryable bool `mapstructure:"only_persist_retryable"`
}
```

**Implementation**:

1. Add an in-memory buffer layer before persistent storage
2. On first send attempt failure:
   - If error is permanent → drop immediately, don't persist
   - If error is retryable → persist to storage for retry
3. Make this opt-in to avoid breaking existing behavior

**Migration Path**:
- v1.0: Add feature as opt-in (`only_persist_retryable: true`)
- v2.0: Make it default behavior
- v3.0: Remove old behavior

---

## 7. Test Plan

### Unit Tests Needed

1. **Test**: Persistent queue + permanent error → item not persisted
2. **Test**: Persistent queue + retryable error → item persisted  
3. **Test**: Persistent queue + eventual success → item removed from storage
4. **Test**: Memory queue + permanent error → item dropped immediately
5. **Test**: Restart with persisted non-retryable items → items not replayed

### Integration Tests Needed

1. **Test**: Full pipeline with OTLP HTTP exporter returning 400 → queue empty
2. **Test**: Full pipeline with OTLP HTTP exporter returning 503 → queue has items
3. **Test**: Storage size doesn't grow indefinitely with permanent errors

---

## 8. Risk Assessment

### Risks of NOT Fixing

- **HIGH**: Production storage fills with garbage data
- **MEDIUM**: Performance degradation from retrying permanent failures
- **MEDIUM**: Misleading observability (queue metrics show "work" that will never succeed)

### Risks of Fixing (Option 2/3)

- **LOW**: Potential breaking change if users rely on current behavior
- **MEDIUM**: Implementation complexity and testing burden
- **LOW**: Performance impact from additional error checking

### Recommended Risk Mitigation

1. Make fix opt-in initially  
2. Add detailed migration guide
3. Provide clear logging when items are dropped due to permanent errors
4. Add metrics to track dropped vs. persisted items

---

## 9. Conclusion

**BUG CONFIRMED**: Issue #14174 is a real architectural problem where the queue persists items before permanent error classification occurs.

**Recommended Action**:
1. Document the limitation immediately
2. Implement "lazy persistence" as an opt-in feature
3. Migrate to lazy persistence as default in next major version

**Priority**: HIGH - Affects production reliability and storage costs

**Complexity**: MEDIUM - Requires architectural changes but well-scoped

**Backward Compatibility**: Can be maintained with opt-in approach
