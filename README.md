# Jalin Training — Camunda Day 5

Proyek pelatihan integrasi **Camunda 8 (Zeebe)** dengan **Spring Boot** dan **Apache Kafka**. Proyek ini mendemonstrasikan dua alur proses BPM: proses pembayaran asinkron berbasis Kafka dan proses routing batch data dengan operasi koleksi.

---

## Daftar Isi

- [Tujuan](#tujuan)
- [Arsitektur](#arsitektur)
- [Tech Stack](#tech-stack)
- [Prasyarat](#prasyarat)
- [Instalasi & Menjalankan](#instalasi--menjalankan)
- [Konfigurasi](#konfigurasi)
- [REST API](#rest-api)
- [Job Workers](#job-workers)
- [Alur Proses](#alur-proses)
- [Simulasi Sistem Eksternal](#simulasi-sistem-eksternal)
- [Struktur Proyek](#struktur-proyek)

---

## Tujuan

Proyek ini dibuat sebagai bahan pelatihan hari ke-5 untuk memahami:

- Cara mengintegrasikan **Camunda 8** dengan Spring Boot menggunakan Zeebe Client
- Cara membuat **Job Worker** untuk menangani service task di BPMN
- Cara menggunakan **Apache Kafka** sebagai message broker dalam orkestrasi proses
- Pola komunikasi **asinkron** antara Camunda dan sistem eksternal
- Manipulasi data koleksi (batch) dalam proses BPM (parse, iterate, read, create/edit)

---

## Arsitektur

```
HTTP Client
    │
    ▼
Spring Boot App (port 8081)
    │
    ├── PaymentController  ──► Camunda Zeebe (BPM_Payment_Process)
    │                               │
    │                         ┌─────┴──────┐
    │                    SendPaymentWorker  WaitPaymentWorker
    │                         │                   ▲
    │                         ▼                   │
    │                   Kafka Topic          Kafka Topic
    │                 payment.request      payment.result
    │                         │                   ▲
    │                         └──► ExternalSystemSimulator
    │
    └── ProcessController ──► Camunda Zeebe (BPM_Routing_Process)
                                    │
                     ┌──────────────┼──────────────┐
                ParseJsonWorker  IterateCollections  CreateOrEditCollection
                ReadFromCollection  GenerateJsonResponse  SendJsonResponse
```

---

## Tech Stack

| Komponen         | Versi / Keterangan                     |
|------------------|----------------------------------------|
| Java             | 21                                     |
| Spring Boot      | 3.5.6                                  |
| Camunda SDK      | 8.8.0 (`camunda-spring-boot-starter`)  |
| Apache Kafka     | via `spring-kafka`                     |
| Spring WebFlux   | Reactive HTTP client                   |
| Lombok           | Boilerplate reduction                  |
| Build Tool       | Maven (Maven Wrapper tersedia)         |

---

## Prasyarat

Pastikan semua komponen berikut sudah berjalan sebelum menjalankan aplikasi:

1. **Java 21** — [Download](https://adoptium.net/)
2. **Camunda 8 Self-Managed** — Zeebe harus berjalan di `localhost`
   - gRPC address: `localhost:26500`
   - REST address: `localhost:8080`
   - Cara tercepat: gunakan [Camunda Docker Compose](https://github.com/camunda/camunda-platform)
3. **Apache Kafka** — berjalan di `localhost:9092`
   - Cara cepat: gunakan Docker
     ```bash
     docker run -d --name kafka -p 9092:9092 \
       -e KAFKA_CFG_NODE_ID=0 \
       -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
       -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
       -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
       -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
       -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
       -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
       bitnami/kafka:latest
     ```
4. **BPMN Process** sudah di-deploy ke Camunda:
   - `BPM_Payment_Process`
   - `BPM_Routing_Process`

---

## Instalasi & Menjalankan

### 1. Clone Repository

```bash
git clone <repository-url>
cd jalin-training-camunda-day5
```

### 2. Build Proyek

```bash
# Linux / macOS
./mvnw clean package -DskipTests

# Windows
mvnw.cmd clean package -DskipTests
```

### 3. Jalankan Aplikasi

```bash
# Linux / macOS
./mvnw spring-boot:run

# Windows
mvnw.cmd spring-boot:run
```

Aplikasi akan berjalan di: `http://localhost:8081`

### 4. Verifikasi

```bash
curl http://localhost:8081/actuator/health
```

---

## Konfigurasi

File konfigurasi utama: `src/main/resources/application.yaml`

```yaml
server:
  port: 8081                     # Port aplikasi

camunda:
  client:
    mode: self-managed
    security:
      plaintext: true
    zeebe:
      enabled: true
      grpc-address: http://127.0.0.1:26500   # Zeebe gRPC
      rest-address: http://127.0.0.1:8080    # Zeebe REST

spring:
  kafka:
    bootstrap-servers: localhost:9092         # Kafka broker
    producer:
      key-serializer: StringSerializer
      value-serializer: StringSerializer
    consumer:
      group-id: camunda-group
```

Sesuaikan `grpc-address`, `rest-address`, dan `bootstrap-servers` jika environment berbeda.

---

## REST API

### Start Payment Process

Memulai instance proses `BPM_Payment_Process`.

```
POST /api/payment/start
Content-Type: application/json
```

**Request Body:**

```json
{
  "orderId": "ORD-12345",
  "amount": 150000
}
```

**Response Sukses (200):**

```json
{
  "processInstanceKey": 2251799813685281
}
```

> **Catatan:** `orderId` yang mengandung kata `"ajojing"` akan disimulasikan sebagai pembayaran **gagal**.

---

### Start Routing Process

Memulai instance proses `BPM_Routing_Process` untuk operasi batch data.

```
POST /api/routing/start
Content-Type: application/json
```

**Request Body:**

```json
{
  "mode": "compare",
  "batchId": "BATCH-001",
  "payloadA": "[{\"trxId\":\"T1\",\"amount\":100000},{\"trxId\":\"T2\",\"amount\":200000}]",
  "payloadB": "[{\"trxId\":\"T2\",\"amount\":200000},{\"trxId\":\"T3\",\"amount\":300000}]"
}
```

**Response Sukses (200):**

```json
{
  "processInstanceKey": 2251799813685299,
  "message": "routing_process started"
}
```

---

## Job Workers

Job Workers adalah komponen yang menangani **service task** di proses BPMN. Setiap worker terhubung ke Zeebe melalui tipe job yang didefinisikan dengan `@JobWorker`.

### 1. `SendPaymentWorker`

| Atribut   | Nilai            |
|-----------|------------------|
| Job Type  | `send-request`   |
| Input     | `orderId`        |
| Output    | —                |

Mengirimkan `orderId` ke Kafka topic `payment.request` untuk diproses oleh sistem eksternal.

---

### 2. `WaitPaymentWorker`

| Atribut   | Nilai            |
|-----------|------------------|
| Job Type  | `wait-result`    |
| Input     | —                |
| Output    | `result` (`"success"` atau `"failed"`) |

Mendengarkan Kafka topic `payment.result` menggunakan `BlockingQueue`. Job akan menunggu hingga ada pesan masuk, lalu mengembalikan hasilnya ke proses Camunda.

---

### 3. `ParseJsonWorker`

| Atribut   | Nilai            |
|-----------|------------------|
| Job Type  | `parse_json`     |
| Input     | `payloadA` (JSON string), `payloadB` (JSON string) |
| Output    | `parsedPayloadA`, `parsedPayloadB`, `parsed`, `status` |

Mem-parse string JSON dari variabel `payloadA` dan `payloadB` menjadi `List<Map>` yang siap digunakan worker berikutnya.

---

### 4. `IterateCollectionsWorker`

| Atribut   | Nilai                |
|-----------|----------------------|
| Job Type  | `iterate_collections` |
| Input     | `parsedPayloadA`, `parsedPayloadB` |
| Output    | `duplicates`, `duplicatesCount`, `action`, `status` |

Membandingkan dua koleksi transaksi berdasarkan `trxId` dan mengembalikan daftar ID yang duplikat.

---

### 5. `ReadFromCollectionWorker`

| Atribut   | Nilai                  |
|-----------|------------------------|
| Job Type  | `read_from_collection` |
| Input     | `parsedPayloadA`, `batchId` |
| Output    | `totalAmount`, `action`, `status` |

Membaca koleksi transaksi dan menghitung total `amount` dari semua item.

---

### 6. `CreateOrEditCollectionWorker`

| Atribut   | Nilai                      |
|-----------|----------------------------|
| Job Type  | `create_or_edit_collection` |
| Input     | `payloadA` (JSON string), `batchId` |
| Output    | `savedBatch`, `action`, `status` |

Membuat atau memperbarui batch koleksi. Menambahkan metadata (`batchId`, `processedAt`) pada setiap item dan menyimpannya ke file JSON di `/tmp/<batchId>_batch.json`.

---

### 7. `GenerateJsonResponseWorker`

| Atribut   | Nilai                    |
|-----------|--------------------------|
| Job Type  | `generate_json_response` |
| Input     | Semua variabel proses    |
| Output    | `response`, `responseJson`, `status` |

Mengumpulkan field-field penting dari variabel proses (`mode`, `batchId`, `status`, `action`, `totalAmount`, dll.) dan membentuknya menjadi JSON response terstruktur.

---

### 8. `SendJsonResponseWorker`

| Atribut   | Nilai               |
|-----------|---------------------|
| Job Type  | `send_json_response` |
| Input     | `response`, `batchId` |
| Output    | —                   |

Menyimpan JSON response final ke file `/tmp/response_<batchId>.json` dan mencetaknya ke log.

---

## Alur Proses

### BPM_Payment_Process

```
Start ──► send-request ──► wait-result ──► [Gateway: result?]
                                               ├── success ──► End (Sukses)
                                               └── failed  ──► End (Gagal)
```

1. Request masuk via `POST /api/payment/start`
2. Worker `send-request` mengirim `orderId` ke Kafka
3. `ExternalSystemSimulator` memproses dan mengirim hasil ke `payment.result`
4. Worker `wait-result` membaca hasil dan mengisi variabel `result`
5. Gateway di BPMN menentukan alur berikutnya berdasarkan nilai `result`

### BPM_Routing_Process

```
Start ──► parse_json ──► [Gateway: mode?]
                            ├── "compare" ──► iterate_collections ──► generate_json_response ──► send_json_response ──► End
                            ├── "read"    ──► read_from_collection ──► generate_json_response ──► send_json_response ──► End
                            └── "upsert"  ──► create_or_edit_collection ──► generate_json_response ──► send_json_response ──► End
```

1. Request masuk via `POST /api/routing/start` dengan variabel `mode`
2. Worker `parse_json` mem-parse semua payload JSON
3. Gateway mengarahkan ke worker yang sesuai berdasarkan `mode`
4. Worker spesifik mengolah data dan mengisi variabel hasil
5. Response dikumpulkan, di-generate, lalu disimpan ke file

---

## Simulasi Sistem Eksternal

`ExternalSystemSimulator` adalah komponen yang mensimulasikan sistem pembayaran eksternal. Komponen ini **otomatis berjalan** bersamaan dengan aplikasi.

**Logika simulasi:**

| Kondisi `orderId`         | Hasil               |
|---------------------------|---------------------|
| Mengandung `"ajojing"`    | `payment failed`    |
| Tidak mengandung `"ajojing"` | `payment success` |

Simulasi menambahkan delay **3 detik** untuk mensimulasikan latency jaringan.

**Contoh test gagal:**
```bash
curl -X POST http://localhost:8081/api/payment/start \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ajojing-123"}'
```

**Contoh test sukses:**
```bash
curl -X POST http://localhost:8081/api/payment/start \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-2025-001"}'
```

---

## Struktur Proyek

```
src/main/java/co/id/jalin/camunda/training/day5/
├── JalinTrainingCamundaDay5Application.java   # Entry point
├── controller/
│   ├── PaymentController.java                 # POST /api/payment/start
│   └── ProcessController.java                 # POST /api/routing/start
├── worker/
│   ├── SendPaymentWorker.java                 # Job: send-request
│   ├── WaitPaymentWorker.java                 # Job: wait-result
│   ├── ParseJsonWorker.java                   # Job: parse_json
│   ├── IterateCollectionsWorker.java          # Job: iterate_collections
│   ├── ReadFromCollectionWorker.java          # Job: read_from_collection
│   ├── CreateOrEditCollectionWorker.java      # Job: create_or_edit_collection
│   ├── GenerateJsonResponseWorker.java        # Job: generate_json_response
│   └── SendJsonResponseWorker.java            # Job: send_json_response
└── util/
    └── ExternalSystemSimulator.java           # Simulasi sistem pembayaran eksternal

src/main/resources/
└── application.yaml                           # Konfigurasi aplikasi
```

---

## Kafka Topics

| Topic             | Producer                    | Consumer                    |
|-------------------|-----------------------------|-----------------------------|
| `payment.request` | `SendPaymentWorker`         | `ExternalSystemSimulator`   |
| `payment.result`  | `ExternalSystemSimulator`   | `WaitPaymentWorker`         |
