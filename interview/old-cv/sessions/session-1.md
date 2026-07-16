
# Opus 4.6 High - Cursor

## Kết quả

### Level Assessment: **Solid Mid-level, approaching Senior**

Tương đương khoảng **Mid+ / Pre-Senior** (2.5-3.5 YoE scale).

---

### Điểm mạnh (What impressed me):

| Area                          | Assessment                                                                                          |
| ----------------------------- | --------------------------------------------------------------------------------------------------- |
| **Debugging methodology**     | Quy trình debug production incident rõ ràng, có hệ thống (symptom → DB → service health → Kafka UI) |
| **Practical problem-solving** | Giải pháp batch processing + grouping + bulk insert là sound engineering, có benchmark batch size   |
| **Idempotency awareness**     | Hiểu rõ at-least-once delivery, implement 2 lớp dedup (app + DB level)                              |
| **Production experience**     | Đã gặp và fix real issues (deadlock/lock contention, consumer lag) — not theoretical                |
| **Pragmatism**                | Biết khi nào apply pattern, khi nào adapt (hexagonal vs existing codebase)                          |

---

### Điểm cần cải thiện (Gaps to Senior):

| Area | Feedback |
|------|----------|
| **Depth of explanation** | Một số câu trả lời còn surface-level. Senior cần giải thích **WHY** sâu hơn — ví dụ tại sao average cost vs FIFO, trade-off cụ thể của mỗi approach |
| **Terminology precision** | "Deadlock" thực chất là lock contention/concurrent write conflict, không phải classic deadlock (circular wait). Senior cần phân biệt chính xác |
| **System design breadth** | Câu scaling chỉ focus vào Kafka + DB connection. Chưa mention: caching layer, read replicas, CQRS pattern, monitoring/alerting strategy, graceful degradation |
| **Trade-off articulation** | Khi đưa ra solution, cần nói rõ hơn "nếu chọn A thì mất gì, chọn B thì mất gì". Ví dụ: batch size lớn → latency cao hơn cho individual messages, batch nhỏ → throughput thấp |
| **Architecture depth** | Giải thích hexagonal còn thiếu insight cốt lõi: domain layer independent từ CẢ driving side (HTTP/gRPC/Kafka) VÀ driven side (DB/cache), không chỉ swap infra |

---

### Recommendations để lên Senior:

1. **Practice articulating trade-offs** — mỗi quyết định kỹ thuật, tập nói "I chose X over Y because Z, accepting the trade-off of W"
2. **Broaden system design vocabulary** — study CQRS, event sourcing, saga pattern, backpressure mechanisms, circuit breaker
3. **Deepen distributed systems knowledge** — CAP theorem applied, consistency models, partition tolerance scenarios cụ thể
4. **Own bigger scope** — Senior không chỉ fix problems mà propose architecture changes proactively, influence cross-team decisions (ví dụ: push harder cho partition key change ở crawler team)

---

### Score breakdown (1-5)

| Criteria            | Score |
| ------------------- | ----- |
| Problem-solving     | 4/5   |
| Technical depth     | 3/5   |
| System design       | 3/5   |
| Communication       | 3.5/5 |
| Production maturity | 4/5   |
| Overall             | 3.5/5 |

---
## Câu 1

**Q: Chào Duy, cảm ơn bạn đã tham gia buổi phỏng vấn. Trước khi đi sâu vào technical, bạn có thể giới thiệu ngắn gọn về bản thân và highlight 1-2 thành tựu kỹ thuật mà bạn tự hào nhất trong 3 năm qua không?**

A: chào, em tên là Trần Mạnh Duy. Em tốt nghiệp trường đại học công nghệ, đại học quốc gia hà nội. Em có 3 năm kinh nghiệm làm việc tại sotatek ở vị trí Backend. Các dự án em tham gia bao gồm các hệ thống giao dịch crypto và phần mềm học tiếng nhật. Trong đó dự án khó khăn nhất là RaidenX là 1 hệ thống giao dịch cho phép người dùng chọn các cặp token và giao dịch trên đó. Phần em đảm nhận chủ yếu là viết các đầu api tổng hợp dữ liệu người dùng trên nhiều blockchain, và tối ưu tốc độ xử lý dữ liệu phía consumer, giảm lag

> I: Chào anh/chị. Em tên Duy, 3 năm kinh nghiệm Backend tại Sotatek, chủ yếu dùng Go cho các hệ thống fintech. Thành tựu em tự hào nhất là ở dự án RaidenX — một DEX aggregator đa blockchain. Em join khi hệ thống đang ở giai đoạn prototype TypeScript single-chain, và em đã drive việc migrate sang Go multi-chain cho 6 blockchains. Cụ thể hơn, em own toàn bộ data pipeline phía consumer: từ Kafka consumption, PnL computation, đến leaderboard ranking. Highlight lớn nhất là resolve một incident consumer lag 4 triệu messages, đưa về steady-state vài trăm — bài toán đó buộc em phải redesign pipeline từ per-message sang batch processing, kết hợp thay đổi cả producer side.

---
## Câu 2 (Deep-dive - Kafka Consumer Lag)

**Q: Bạn nói đến việc tối ưu tốc độ xử lý phía consumer và giảm lag. Trong CV cũng mention đến việc xử lý backlog ~4 triệu messages.** 

**Bạn có thể walk through cho mình:**
1. **Khi bạn nhận ra vấn đề consumer lag đó, triệu chứng ban đầu là gì? Bạn phát hiện ra bằng cách nào?**
2. **Root cause cụ thể là gì khiến lag lên đến hàng triệu messages?**

A: vấn đề lag được raise khi mà user/tester thực hiện giao dịch mà không có thông tin vị thế giao dịch hiển thị trên màn hình. Sau khi nhận thông tin, em thực hiện kiểm tra dữ liệu trong database. Chưa có dữ liệu nên em kiểm tra dashboard của argo cd, consumer log, mục đích là muốn xem consumer còn hoạt động hay không. sau khi thấy consumer healthy và log không có error thì em xem tiếp kafka ui, ở đây em thấy consumer lag khá lớn là khoảng vài triệu message.

Sau khi xem lại log của service và của các crawler, thì em nhận ra tốc độ xử lý của consumer chưa đuổi kịp tốc độ produce của các crawler, lượng dữ liệu được lấy trên nhiều blockchain trả về kafka là khá lớn. Em xác định đây là gốc rễ vấn đề

> I: Triệu chứng ban đầu là user report: giao dịch xong nhưng position không hiển thị. Em nghĩ ngay đến 2 khả năng: hoặc write path fail (consumer crash), hoặc write path chậm (lag). Em check DB trước — data chưa có. Tiếp theo em check ArgoCD và consumer logs — service healthy, không error. Đến đây em gần như chắc là lag issue, nên em vào Kafka UI confirm — consumer lag khoảng 4 triệu.
> 
> Root cause: pipeline lúc đó xử lý per-message — mỗi transaction đi qua full pipeline: validate → compute → single insert → commit. Với 6 blockchain crawlers produce cùng lúc, throughput produce khoảng vài chục nghìn msg/s, trong khi consumer chỉ xử lý được vài trăm msg/s do I/O bound ở DB writes. Bottleneck chính là DB round-trip per message, không phải CPU computation.

---
## Câu 3 (Follow-up - Root cause deeper)

**Q: Bạn nói root cause là consumer xử lý không kịp tốc độ produce. Nhưng mình muốn đi sâu hơn:**

**Consumer chậm ở đâu cụ thể? Khi bạn nhìn vào code/logic của consumer lúc đó, bottleneck nằm ở chỗ nào? Ví dụ:**
- **Có phải mỗi message đang gọi DB một lần?**
- **Hay logic tính toán quá nặng?**
- **Hay do cách Kafka consumer được configure (single consumer, commit strategy...)?**

**Và giải pháp cụ thể bạn đã áp dụng là gì để đưa lag từ hàng triệu xuống còn vài trăm?**

A: thứ nhất là về consumer. pipeline của consumer lúc đó như sau: lấy dữ liệu từ kafka, làm sạch để loại bỏ các message lỗi format/tràn số, tính toán các field pnl/roi, insert vào db, commit offset. format message được xử lý là 1 transaction. có thể thấy với mỗi transaction lại phải đi lại pipeline đó từ đầu, trong khi số lượng transaction là khá lớn.

Từ đó em đưa ra giải pháp xử lý batch, lấy size là 1000 message, kết hợp với group theo userID/symbol để loại bỏ bớt lượng data unused, và bulk insert vào DB. ngoài đó ra em đề xuất 1 thay đổi phía crawler, đó là đẩy message có nhiều transaction, thay vì 1 transaction/1 message.

Với kết hợp 2 giải pháp trên, con số lag giảm xuống vài nghìn cho đến vài trăm sau vài tiếng update và ổn định ở mức đó

> I: Em attack từ 2 hướng. Hướng 1 — consumer side: chuyển từ per-message sang batch processing. Cụ thể: consumer poll lên đến 1000 messages/batch, group theo userID+symbol, sort theo timestamp trong mỗi group để giữ ordering, rồi bulk insert. Batch size 1000 là kết quả em benchmark — dưới 500 thì throughput chưa saturate, trên 2000 thì bulk insert bắt đầu timeout do payload quá lớn và tăng latency cho mỗi batch.
> 
> Hướng 2 — producer side: đề xuất crawler team gom nhiều transactions vào 1 Kafka message thay vì 1:1. Điều này giảm message count mà không thay đổi data volume, giúp consumer poll ít lần hơn.
> 
> Trade-off em accept: batch processing tăng latency cho individual transaction — worst case 1 transaction phải chờ cả batch đầy mới được process. Em mitigate bằng cách thêm timeout: nếu chưa đủ 1000 messages mà đã chờ 500ms thì flush batch luôn. Như vậy max latency thêm là 500ms, acceptable cho use case này vì user không cần real-time tới millisecond.

---
## Câu 4 (Follow-up - Trade-offs & Edge cases)

**Q: Khi chuyển sang batch processing, có một số trade-off và edge case mình muốn hỏi:**
1. **Batch size 1000 — con số này bạn chọn dựa trên cơ sở nào? Có benchmark hay thử nghiệm gì không, hay là chọn empirical?**
2. **Ordering guarantee: Bạn nói group theo userID/symbol. Nhưng khi batch 1000 messages từ nhiều partitions, làm sao bạn đảm bảo thứ tự xử lý đúng cho cùng 1 user? Ví dụ user A mua rồi bán, nếu xử lý sai thứ tự thì PnL sẽ sai. Bạn handle case này thế nào?**
3. **Failure handling: Nếu batch 1000 messages mà insert DB fail ở giữa (ví dụ 500 records thành công, 500 fail), bạn xử lý thế nào? Commit offset hay retry toàn bộ batch?**

A: về batch size, sau khi thử nghiệm và đánh giá tốc độ trên nhiều giá trị khác nhau: từ 10000 - 50, em thấy 1000 là con số phù hợp, lớn hơn 1000 có thể gặp vấn đề về insert db, nhỏ tầm vài trăm lại không đạt tốc độ xử lý tối ưu.

về case này, lúc đó em group theo userID/symbol và sort theo field timestamp-ms được provide trong message đó. Em cũng đã đề xuất update crawler sử dụng patrition key là userID/symbol thay vì setting 1 số lượng ngẫu nhiên khoảng 20-25 patrition, như thế khi 1 consumer xử lý từ 1 patrition sẽ đảm bảo ordering hơn so với sort theo timestamp-ms. Tuy nhiên có nhiều trade-off mà teamate làm crawler chưa update được, nên sort theo timestamp-ms được provide trong message là phương án tối ưu nhất em có thể làm.

Trong trường hợp fail giữa phase insert DB, em sẽ không commit offset mà retry toàn bộ batch. Nếu commit luôn thì sẽ mất data.

> Ordering: Lý tưởng thì partition key nên là userID hoặc userID+symbol — Kafka guarantee ordering within partition, nên cùng 1 user sẽ luôn đúng thứ tự. Em đã propose điều này nhưng crawler team có constraints riêng — họ partition theo blockchain để balance load đều giữa các brokers. Nên hiện tại em handle ở consumer: sau khi group by userID+symbol, em sort theo timestamp_ms trong message. Đây là best-effort ordering — nếu 2 transactions cùng timestamp thì có thể sai thứ tự, nhưng trong thực tế blockchain timestamp granularity đủ để phân biệt.
> 
> Failure handling: Khi batch insert fail, em retry toàn bộ batch mà không commit offset. Điều này có nghĩa là at-least-once delivery — records đã insert sẽ bị insert lại. Em handle idempotency bằng ON CONFLICT (transaction_id, user_id) DO NOTHING ở DB level. Nên retry an toàn — duplicate sẽ bị ignore, records chưa insert sẽ được insert.
> 
> Em cũng set retry limit — 3 lần. Nếu vẫn fail thì publish batch vào dead-letter topic để investigate sau, rồi skip và commit offset. Tránh 1 bad batch block toàn bộ pipeline.

---
## Câu 5 (Follow-up - Idempotency)

**Q: Bạn nói retry toàn bộ batch khi DB insert fail. Vậy câu hỏi tiếp theo:**
**Khi retry lại cả batch, những records đã insert thành công trước đó sẽ bị insert lại lần nữa. Bạn xử lý idempotency ở đây thế nào?**
- **Dùng unique constraint rồi upsert?** 
- **Hay deduplication ở application level?**
- **Key để deduplicate là gì — dựa vào field nào trong message?**
**Và mở rộng hơn: ngoài case retry batch, bạn còn gặp case nào khác mà at-least-once delivery gây ra duplicate processing không? Bạn handle ra sao ở tầng application?**

A: ở phase query, em có đặt điều kiện on conflict gồm transaction id và user id. trước phase đó, phase group cũng dedup theo userID/symbol, khi group có transaction id trùng thì sẽ loại bỏ.

ngoài case này, việc duplicate processing có thể xảy ra khi mà không có logic kiểm tra db xem transaction id đã có hay chưa, hoặc insert db failed nhưng vẫn commit offset. ở tầng application, em sẽ luôn đảm bảo ignore khi lặp transaction id, và chỉ commit offset khi đã hoàn thành pipeline

> Em implement idempotency ở 2 layers vì defense-in-depth:
> 
> Layer 1 — Application: Trong phase group, nếu cùng 1 transaction_id xuất hiện nhiều lần trong batch (có thể do retry hoặc producer duplicate), em chỉ giữ bản mới nhất theo timestamp. Điều này giảm unnecessary DB writes.
> 
> Layer 2 — Database: ON CONFLICT (transaction_id, user_id) DO NOTHING. Đây là last line of defense — dù application miss duplicate nào, DB sẽ reject.
> 
> Lý do cần cả 2: nếu chỉ rely on DB, mỗi duplicate vẫn tốn 1 round-trip để DB check conflict rồi reject. Với batch lớn, số duplicate có thể significant, nên filter trước ở app level tiết kiệm I/O. Ngược lại nếu chỉ rely on app, cross-batch duplicate (transaction nằm ở 2 batch khác nhau do retry) sẽ miss — chỉ DB constraint mới catch được.

---
## Câu 6 (Chuyển topic - PnL Computation)

**Q: Okay, mình chuyển sang phần PnL/profit recompute. Đây là phần bạn mô tả là "money-critical stream".**
1. **Logic tính PnL: Bạn tính PnL cho user như thế nào? Ví dụ user mua 1 ETH ở giá $2000, rồi mua thêm 0.5 ETH ở $2500, rồi bán 0.8 ETH ở $2800. PnL được tính theo phương pháp nào (FIFO, average cost, realized vs unrealized)?**
2. **"Recompute": Tại sao cần recompute? Trong trường hợp nào PnL cần được tính lại thay vì chỉ tính incremental khi có transaction mới?**

A: em có dùng average cost để tính cost basis. mỗi giao dịch mới sẽ cập nhập position, avg entry price, realized/unrealized pnl lấy theo giá thị trường hiện tại, được provide trong message kafka.

realtime thì em tính theo incremental để giảm độ trễ. Tuy nhiên vẫn cần recompute khi transaction history bị thay đổi, giả dụ như order của transaction chưa đúng, hoặc backfill dữ liệu

> Em dùng average cost basis cho cost calculation. Lý do chọn average cost thay vì FIFO: trong crypto DEX trading, user thường trade fractional amounts rất nhiều lần — FIFO yêu cầu maintain lot-level tracking cho mỗi buy, phức tạp hơn và storage-intensive hơn. Average cost đơn giản hơn: chỉ cần track total_quantity và total_cost, avg_price = total_cost / total_quantity. Trade-off là kém chính xác hơn FIFO cho tax reporting, nhưng đây là trading platform, không phải tax software, nên acceptable.
> 
> Real-time path: Mỗi transaction mới → update position incremental: adjust quantity, recalculate avg price nếu buy, compute realized PnL nếu sell (sell_price - avg_cost) * sell_quantity. Unrealized PnL lấy market price từ Kafka message.
> 
> Recompute path: Cần khi transaction history bị retroactively thay đổi — ví dụ blockchain reorg (transactions bị revert), hoặc crawler backfill thêm historical data mà trước đó miss. Lúc này incremental state không còn đúng, phải replay từ đầu.

---
## Câu 7 (Follow-up - Recompute mechanism)

**Q: Khi cần recompute cho 1 user:**
1. **Trigger recompute thế nào? Ai/cái gì quyết định "user này cần recompute"? Có 1 message riêng trên Kafka hay là API call, hay scheduled job?**
2. **Performance: Giả sử 1 user có 10,000 transactions trên 1 symbol. Khi recompute, bạn phải replay lại tất cả 10K transactions theo thứ tự để tính lại avg cost + PnL. Bạn có gặp vấn đề performance ở đây không? Có optimize gì (snapshot/checkpoint) hay cứ replay từ đầu?**
3. **Consistency: Trong lúc đang recompute cho user A, nếu có transaction mới của user A đến từ Kafka stream, bạn handle thế nào để tránh race condition giữa recompute và incremental update?**

A: recompute được tính theo scheduled job chạy nền

job sẽ không quan tâm đến transaction, nó sẽ lấy hẳn dữ liệu position được lưu trong DB. quy trình gồm tìm các position chưa được sync, dùng api bên thứ 3 để lấy giá, tính lại các field, mark các position đó đã sync và sẽ lặp lại trong 1 duration sắp tới.

từng có trường hợp xảy ra deadlock vì vấn đề này. sau này được em fix bằng cách thêm 1 field để thể hiện ready to recompute, trong quá trình thực hiện recompute thì có dùng transaction kết hợp row lock. vì thế nên không còn bị race nữa.

---
## Câu 8 (Follow-up - Deadlock detail)

**Q: Nhanh thôi về deadlock đó:**
**Bạn có thể mô tả cụ thể scenario gây deadlock không? Ví dụ: goroutine A lock row X rồi chờ row Y, trong khi goroutine B lock row Y chờ row X — hay là pattern khác?**
**Và "row lock" bạn dùng ở đây cụ thể là gì — `SELECT ... FOR UPDATE`? Trên DB nào (Postgres hay Mongo)?**

A: em dùng postgres. deadlock không phải kiêu a giữ x chờ y, b giữ y chờ x, mà do nhiều worker cùng lúc recompute cho cùng 1 position nên cùng update 1 row. Có lúc transaction bị postgres phát hiện deadlock hoặc lock wait khá lâu.

em xử lý bằng select for update để chỉ một transaction được sửa position tại 1 thời điểm, đồng thời thêm 1 flag field để tránh nhiều worker cùng pick 1 position. sau khi update không còn gặp tình trạng deadlock nữa

> Chính xác hơn thì đây là write contention hơn là classic deadlock. Scenario: scheduled job spawn N workers, mỗi worker query "positions WHERE needs_recompute = true LIMIT 100". Khi nhiều workers cùng pick overlapping positions, chúng cùng attempt UPDATE trên cùng row → Postgres detect lock wait timeout hoặc trong một số case rare, actual deadlock khi 2 transactions lock rows theo thứ tự ngược nhau.
> 
> Em fix bằng 2 mechanisms:
> 
> 1. Claim before process: Worker dùng `UPDATE positions SET claimed_by = $worker_id, claimed_at = now() WHERE needs_recompute = true AND claimed_by IS NULL LIMIT 100 RETURNING *`. Atomic claim — nếu 2 workers race, chỉ 1 thắng nhờ row-level lock của UPDATE itself.
> 2. SELECT FOR UPDATE SKIP LOCKED cho individual position khi process — nếu row đang bị lock bởi worker khác (edge case), skip thay vì wait.
> 
> Thêm safeguard: claimed_at có TTL — nếu worker crash sau khi claim mà chưa process xong, 1 cleanup job sẽ reset claim sau 5 phút.

---
## Câu 9 (Chuyển topic - Price Alert State Machine)

**Q: Trong CV bạn mention "per-user, preference-driven price-alert state machine that prevented duplicate alerts". Mình muốn hiểu design này:**
1. **State machine gồm những states nào? Transitions giữa các states là gì? Ví dụ: user đặt alert "ETH > $3000" → flow đi qua những states nào từ lúc tạo alert đến lúc gửi notification?**
2. **"Prevented duplicate alerts" — cụ thể duplicate xảy ra thế nào nếu không có state machine? Ví dụ giá ETH dao động quanh $3000 (lên xuống liên tục), bạn handle thế nào để user không bị spam notification?**

A: gồm có 2 trạng thái là triggered và safe. logic sẽ xoay quanh 2 biến là was_triggered_before và is_triggered_now. transition bao gồm 3 loại chính: giữ nguyên state -> không làm gì để tránh spam, từ safe -> triggered khi mà giá hiện tại đã vào vùng cảnh báo nhưng trước đó chưa fire noti -> bây giờ sẽ fire noti, từ triggered -> safe là khi giá hiện tại đã ra khỏi vùng cảnh báo, sẽ reset trạng thái và lưu giá hiện tại vào memory.

với logic thông thường là compare giá hiện tại và giá setting, thì khi user setting là cảnh báo khi dưới 3000, thì mỗi khi có message kafka có base_value dưới giá này thì sẽ fire noti liên tục.

Để tránh bị spam, thì ngoài việc cài đặt state machine, em thêm các cache value như can send value có ttl và count noti để gửi thông báo gộp sau 1 khoảng thời gian chờ nhất định

---
## Câu 10 (Architecture - Hexagonal)

**Q: Bạn mention sử dụng hexagonal architecture (ports & adapters) trong project Japanese-learning app. Câu hỏi:**
1. **Bạn có thể giải thích tại sao chọn hexagonal cho project đó thay vì một structure đơn giản hơn (ví dụ 3-layer: handler → service → repository)?**
2. **Cho mình 1 ví dụ cụ thể trong project đó: 1 port (interface) và adapter tương ứng là gì? Khi nào việc tách port/adapter mang lại giá trị thực sự so với direct dependency?**
3. **Trong context RaidenX (Kafka consumer pipeline), bạn có apply hexagonal không? Nếu có thì structure thế nào, nếu không thì tại sao?**

A: thứ nhất, kiến trúc đó tường minh về mặt codebase, khi mà em quay lại dự án sau 1 khoảng thời gian thì em hiểu được ngay codebase đang có những gì, code nào phụ thuôc đâu. thứ 2 là khả năng scale, em muốn dự án có thể thử nghiệm và đánh giá nhiều infra và chọn ra phù hợp, nên việc dễ dàng cài đặt với một infra mới là cần thiết.

trong dự án, em cài đặt port Word và Adapter Mongo/Redis. Việc tách port-adapter làm service layer thuận tiện hơn khi dùng nhiều infra. nếu mà direct, lớp handler sẽ có nhiều business logic vì nó là lớp phải tương tác gián tiếp với infra.

trong raidenx lại không dùng kiến trúc này, lý do thì em nghĩ lớp infra đã được quyết định trước, thiết kế đã được triển khai, khi em join dự án cần phải apply vào kiến trúc đã có để thích nghi với dự án nhanh hơn và đẩy nhanh quá trình phát triển

---
## Câu 11 (System Design - Broader thinking)

**Q: Câu cuối cùng, mang tính thiết kế hệ thống hơn:**
**Giả sử bây giờ RaidenX scale lên 50 blockchains thay vì 6, và số lượng users tăng 10x. Consumer lag bắt đầu quay lại.**
**Với kinh nghiệm hiện tại, bạn sẽ redesign pipeline này thế nào để handle scale đó? Hãy nghĩ về:**
- **Kafka topology (partitions, topics, consumer groups)**
- **Horizontal scaling strategy cho consumers**
- **Có cần tách service không, hay giữ monolithic consumer?**
- **Có bottleneck nào khác ngoài consumer processing speed (ví dụ DB writes, network...)?**
**Bạn không cần trả lời hết, focus vào 2-3 điểm bạn thấy quan trọng nhất.**

A: số lượng blockchain khá lớn, về phía ingestor em sẽ deploy nhiều producer, có thể mỗi blockchain 1 producer, format vào 1 schema chung và đẩy chung vào 1 topic, trong topic có thể chia patrition theo entity key là userID, hoặc userID/symbol. tùy theo số lượng patrition thì có thể triển khai lượng consumer tương ứng. sẽ setting để tránh việc rebalance toàn cục broker khi có user mới

pipeline consumer có thể vẫn như cũ, bây giờ triển khai mỗi consumer 1 patrition để tăng tốc độ xử lý, cũng như đảm bảo order. kết hợp pgbouncer làm trung gian để tránh mở 1 lượng lớn connection vào postgre khiên DB bị chậm.

> Em sẽ focus 3 điểm quan trọng nhất:
> 
> 1. Kafka topology: 50 blockchains → em vẫn giữ single topic cho normalized transaction data (không split per-chain topic vì sẽ phải manage 50 consumer groups). Partition key = userID để guarantee per-user ordering. Số partitions scale theo throughput cần thiết — mỗi partition handle khoảng X msg/s, estimate total throughput rồi chia ra. Nếu cần 200 partitions thì deploy 200 consumer instances trong cùng consumer group.
> 
> 2. Consumer scaling — horizontal + vertical: Horizontal: tăng consumer instances (1:1 với partitions). Vertical: trong mỗi consumer, em có thể parallelize computation phase (PnL calc) vì nó CPU-bound và stateless — chỉ serialize ở DB write phase. Pattern: poll batch → fan-out compute to goroutine pool → fan-in → bulk write. Cẩn thận: phải giữ ordering trong fan-in cho cùng user.
> 
> 3. DB write bottleneck: 10x users + 50 chains → write volume tăng rất lớn. PgBouncer giải quyết connection pooling, nhưng chưa đủ. Em sẽ cần: (a) Write batching lớn hơn + async flush, (b) Partitioned tables theo time range hoặc user_id range để giảm index contention, (c) Consider CQRS — write path vào Postgres, read path serve từ Redis/materialized view. User đọc PnL thường xuyên hơn write, nên tách read/write model có lợi.
> 
> Bonus — Backpressure: Nếu consumer vẫn không kịp dù đã scale, em cần backpressure mechanism — ví dụ monitor lag, khi lag vượt threshold thì alert hoặc auto-scale consumer instances. Tránh để lag tích lũy rồi mới phát hiện như lần trước.
