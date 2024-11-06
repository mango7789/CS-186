package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        List<LockContext> contextToLock = new ArrayList<>();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // If the required lock is NL, just do nothing and return
        if (requestType == LockType.NL) {
            return;
        }
        // Request a shared lock
        else if (requestType == LockType.S) {
            // The parent of the lock is valid, just need to acquire in current lock
            if (effectiveLockType == LockType.S || effectiveLockType == LockType.X) {
                if (explicitLockType == LockType.IS) {
                   lockContext.escalate(transaction);
                } else if (explicitLockType == LockType.IX) {
                    List<ResourceName> releaseResourceNames = new ArrayList<>();
                    releaseResourceNames.add(lockContext.name);
                    for (LockContext child : lockContext.children.values()) {
                        LockType childType = child.getExplicitLockType(transaction);
                        if (childType != LockType.NL && !LockType.canBeParentLock(LockType.SIX, childType)) {
                            releaseResourceNames.add(child.name);
                        }
                    }
                    lockContext.lockman.acquireAndRelease(transaction, lockContext.name, LockType.SIX, releaseResourceNames);
                } else if (explicitLockType == LockType.NL) {
                    lockContext.lockman.acquire(transaction, lockContext.name, requestType);
                }
            }
            else {
                // There is no lock held in parent
                while (parentContext != null && parentContext.getExplicitLockType(transaction) == LockType.NL) {
                    contextToLock.add(parentContext);
                    parentContext = parentContext.parentContext();
                }
                if (!contextToLock.isEmpty()) {
                    Collections.reverse(contextToLock);
                    for (LockContext context : contextToLock) {
                        context.acquire(transaction, LockType.IS);
                    }
                }
                // Acquire the lock in desired resource
                lockContext.acquire(transaction, requestType);
            }
        }
        // Request a exclusive lock
        else {
            if (effectiveLockType == LockType.X) {
                if (explicitLockType == LockType.IX) {
                    lockContext.escalate(transaction);
                } else {
                    lockContext.lockman.acquireAndRelease(transaction, lockContext.name, requestType, Collections.singletonList(lockContext.name));
                }
            } else if (effectiveLockType == LockType.S) {
                while (parentContext != null && parentContext.getExplicitLockType(transaction) != LockType.IX) {
                    contextToLock.add(parentContext);
                    parentContext = parentContext.parentContext();
                }
                if (!contextToLock.isEmpty()) {
                    Collections.reverse(contextToLock);
                    for (LockContext context : contextToLock) {
                        context.promote(transaction, LockType.IX);
                    }
                }
                if (explicitLockType == LockType.NL) {
                    lockContext.acquire(transaction, requestType);
                } else {
                    lockContext.promote(transaction, requestType);
                }
            } else {
                while (parentContext != null && parentContext.getExplicitLockType(transaction) == LockType.NL) {
                    contextToLock.add(parentContext);
                    parentContext = parentContext.parentContext();
                }
                if (!contextToLock.isEmpty()) {
                    Collections.reverse(contextToLock);
                    for (LockContext context : contextToLock) {
                        context.acquire(transaction, LockType.IX);
                    }
                }
                // Acquire the lock in desired resource
                lockContext.acquire(transaction, requestType);
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
}
