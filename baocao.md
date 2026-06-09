# Báo cáo dự án: Apache Kafka

## Tổng quan

Dự án là mã nguồn đầy đủ của Apache Kafka (multi-module Gradle). Mục tiêu của báo cáo này là tóm tắt cấu trúc, các module chính, phân tích dependency cơ bản và bước thực thi cần thiết để build/run.

## Yêu cầu môi trường

- Java (JDK) phiên bản tương thích (dự án hỗ trợ JDK 17 và 25 theo README).
- Gradle wrapper có sẵn trong repository (`gradlew`).

## Module chính (theo `settings.gradle`)

- `clients` (thư viện client: Producer/Consumer/Admin)
- `core` (core protocol, định nghĩa message và API nội bộ)
- `server` (broker runtime)
- `streams` (Kafka Streams API và các submodule)
- `connect` (Kafka Connect: api, runtime, transforms, v.v.)
- `storage` (log & tiered storage)
- `metadata` (controller metadata + codegen)
- `raft` (Raft-based controller)
- `group-coordinator`, `transaction-coordinator`, `server-common`, `tools`, `generator`, `test-common`, `jmh-benchmarks`, `trogdor`, v.v.

## Các dependency quan trọng (từ `gradle/dependencies.gradle`)

- `lz4-java` = 1.10.2 (nén LZ4)
- `zstd-jni` = 1.5.6-10 (Zstandard JNI)
- `snappy-java` = 1.1.10.7
- `slf4j` = 1.7.36, `log4j2` = 2.25.4
- `jackson` = 2.21.2
- `rocksdbjni` = 10.1.3
- `protobuf` = 3.25.5
- `testcontainers` = 1.20.2

Chi tiết các dependency per-module được cấu hình trong `build.gradle` (root) trong các block `project(':clients')`, `project(':core')`, v.v.

## Vấn đề / TODO nổi bật

- `clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockOutputStream.java`: còn TODO cho `uncompressed content size` và `content checksum` (cần hoàn thiện frame compatibility).
- `clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockInputStream.java`: chú thích phải verify content checksum.
- Nhiều `TODO`/`FIXME` khác phân tán trong `connect`, `streams`, `core` (chủ yếu tối ưu hoá, flaky tests, hoặc công việc theo KAFKA-xxxx).

## Các lệnh thường dùng để xây và kiểm tra

- Liệt kê tasks Gradle:

```bash
./gradlew tasks
```

- Liệt kê dependency tree (ví dụ module `clients` runtime):

```bash
./gradlew :clients:dependencies --configuration runtimeClasspath
```

- Build JAR cho toàn repo (có thể tốn thời gian):

```bash
./gradlew jar
```

- Build chỉ module `clients` (nhanh hơn):

```bash
./gradlew :clients:jar
```

- Chạy unit/integration tests (ví dụ module `clients`):

```bash
./gradlew :clients:test
```

- Chạy OWASP dependency-check (plugin đã cấu hình):

```bash
./gradlew :clients:dependencyCheckAnalyze
```

## Đề xuất bước tiếp theo

1. Chạy `:clients:dependencies` để xác nhận tree dependency.
2. Chạy `:clients:dependencyCheckAnalyze` để kiểm tra CVE cho các dependency.
3. Nếu cần tạo báo cáo TODO chi tiết, tôi có thể xuất toàn bộ kết quả `TODO/FIXME` ra file `TODOs.md`.
4. Nếu muốn tôi có thể cố build `:clients:jar` hoặc thực thi một bộ test nhỏ — cho biết module và phạm vi.

## Files đã xem nhanh trong bản báo cáo này

- [README.md](README.md#L1)
- [build.gradle](build.gradle#L1)
- [clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockOutputStream.java](clients/src/main/java/org/apache/kafka/common/compress/Lz4BlockOutputStream.java#L1)

---

_Báo cáo được tạo tự động. Nếu bạn muốn mở rộng nội dung (CSV dependency, danh sách TODO xuất ra file, hay chạy quét CVE), hãy cho tôi biết lựa chọn._
