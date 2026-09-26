package dev.marcosjesus.orderevents.billing;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface BillingRecordRepository extends JpaRepository<BillingRecord, UUID> {
}
