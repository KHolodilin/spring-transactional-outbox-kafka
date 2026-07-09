package com.kholodilin.outbox.events;

/** Logical ACTIVE/ARCHIVE split for hot-path publisher queries vs historical SENT rows. */
public enum PartitionState {
    ACTIVE,
    ARCHIVE
}
