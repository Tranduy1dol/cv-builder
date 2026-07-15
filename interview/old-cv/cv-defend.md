# CV Defend

# RaidenX

## Giới thiệu

Đây là dự án em làm lâu nhất. RaidenX là một nền tảng giao dịch tài sản số hoạt động trên nhiều blockchain.

Ban đầu hệ thống chỉ hỗ trợ một blockchain và chủ yếu viết bằng TypeScript. Khi em vào thì team bắt đầu chuyển một phần sang Go đồng thời mở rộng để hỗ trợ thêm nhiều blockchain khác.

Em làm ở phía xử lý dữ liệu giao dịch. Sau khi hệ thống thu thập các giao dịch từ blockchain và đưa vào Kafka, service của em sẽ xử lý các giao dịch đó để tính toán các chỉ số như PnL, ROI, dữ liệu trending và price alert phục vụ người dùng.

---

## Owned the consumer-side data pipeline end to end

Trong dự án này em own service `insight` — trả về dữ liệu giao dịch / ví của user: PnL, ROI, số lượng token trong ví và quy đổi token ↔ USD. Ngoài ra còn own trending (ranking token) và price-alert theo preference của user. Nguồn data đều từ ingestor đẩy lên Kafka; em build consumer tính toán và setup theo từng functional.

Service được thiết kế **event-driven**. Ingestor (do thành viên khác trong team) lấy data on-chain, chuẩn hoá thành message trên các Kafka topic. Phía em có các consumer:

- Tính **volume** (buy/sell), **balance** (USD / base token), **PnL** theo ví
- Tính cùng các chỉ số đó theo từng **vị thế** mua/bán đã khớp
- Consumer **trending** và **price-alert** cũng consume từ cùng kiểu source topic

Team khoảng 8–10 backend. Em sở hữu end-to-end consumer-side path: Kafka → compute → persist → serve (Postgres cho số đã materialize, Redis cho ranking / sync state).

### Follow-up

**At-least-once / exactly-once, commit offset manual, idempotence**

Em thường không cố exactly-once ở Kafka. Consumer chịu được message phát lại: ghi DB idempotent, offset commit sau khi ghi thành công. Crash + replay không đếm trùng. Producer: cân nhắc idempotent producer và `max.in.flight = 1` khi cần giữ ordering lúc retry.

**Dedup trong batch**

Trong một batch poll về, build map theo tx hash / event id trước khi compute — trùng trong batch còn một bản. Rồi gom theo user/symbol. Dedup trong batch + ON CONFLICT ở DB = hai lớp.

**Deploy trên GKE-prod — bao nhiêu pod / instance**

Mỗi chain một topic riêng; topic trading-data khoảng ~20–25 partition (ops/collector set lúc tạo topic). Consumer insight deploy theo functional + theo chain — pattern 1 pod mỗi consumer deployment mỗi chain; consumer hot hơn scale thêm replica. API có HPA tầm 1→10. Constraint: số consumer instance trong group ≤ số partition. Bottleneck thường là DB write / hot key, không phải thiếu pod.

**Hot-key (ví cá voi)**

Ví / token rất active làm map in-memory hoặc Redis nóng. Theo dõi lag + CPU; I/O-bound → bulk upsert / shard đường ghi. Batching gom event cá voi trong batch; hot partition thì chấp nhận latency cao hơn cho key đó — correctness vẫn giữ vì recompute idempotent.

---

## Re-architected the insight trading-data service

Đây là plan mở rộng sản phẩm. Prototype TypeScript trên Sui launch khá thành công, nhưng để handle throughput cao hơn và đa chain thì PM + CTO thống nhất migrate sang Golang. Em đẩy phần lớn migration kiến trúc ban đầu trước khi team scale lên 8–10 người.

- Prototype TS: đồng bộ hơn, bó chặt giả định của Sui — thêm chain kiểu copy-paste sẽ nhân bug
- Quyết định: **Go + event-driven** — consume event đã chuẩn hoá từ Kafka; tách ingest / compute / serve
- **Normalization boundary** — đặc thù chain (đọc swap, decimals, địa chỉ) nằm sau adapter; phía trên chỉ còn `(ví, token, amount, chiều, timestamp)`
- Launch `gke-prod`; mỗi chain một topic riêng

### Follow-up

**Vì sao Go thay vì giữ Node/TypeScript?**

Cả nền tảng là Go, nên giữ Node nghĩa là một hòn đảo lạc lõng phải vận hành. Go cũng cho em concurrency rẻ cho các consumer throughput cao và footprint deploy/K8s đơn giản hơn. Không phải 'Node dở' — mà là tính nhất quán cộng với sự phù hợp hơn cho một consumer nặng về throughput.

**Event-driven" ở đây cụ thể nghĩa là gì — nguồn sự kiện và hình dạng của nó?**

Nguồn sự kiện là Kafka. Indexer publish các sự kiện balance-change/swap đã chuẩn hoá về một schema chung. `insight` phản ứng với các sự kiện đó thay vì pull hay tính đồng bộ — nên ingest, compute và serve tách rời, mỗi phần hỏng và hồi phục độc lập.

**Cấu trúc service bên trong thế nào (ingest/compute/serve)?**

Ba đường ranh giới: một tầng ingest đọc Kafka và chuẩn hoá, một tầng compute sở hữu logic PnL/ranking/alert và state, và một tầng serve (API) đọc kết quả đã materialize. Compute không bao giờ đụng wire format; serve không bao giờ đụng Kafka.

**Bạn chuẩn hoá decimals/số lượng token qua các chuỗi thế nào mà không bị lỗi độ chính xác?**

Em chuẩn hoá mọi thứ về biểu diễn số nguyên fixed-precision với decimals rõ ràng cho từng token, không bao giờ dùng float. Mỗi adapter chịu trách nhiệm chuyển số lượng thô của chuỗi mình sang dạng canonical đó, nên lõi không bao giờ phải đoán decimals — nó được cho biết ở ranh giới.

**Một chuỗi mới gửi sự kiện lỗi hoặc sai thứ tự — chuyện gì xảy ra?**

Sự kiện lỗi bị validate ở ranh giới ingest và bị reject thay vì làm hỏng cả batch. Sai thứ tự thì được xử lý vì PnL là một phép tính lại từ balance-change gom theo user, và em commit offset thủ công — nên một sự kiện xấu không làm hỏng state của user, nó chỉ bị bỏ qua và đánh dấu.

**Bạn đánh đổi gì khi chọn event-driven thay vì request/response?**

Event-driven đánh đổi sự đơn giản và tính nhất quán tức thời — kết quả là eventually consistent và debug là 'lần theo event' thay vì stack trace. Em đổi cái đó lấy sự tách rời, khả năng replay, và chịu được back-pressure, những thứ quan trọng hơn với một data pipeline so với độ tươi sub-giây.

**Thêm chuỗi thứ 6, thứ 10 ảnh hưởng tải và chi phí ra sao? Nút thắt ở đâu?**

Thêm chuỗi chủ yếu là thêm partition và thêm consumer instance — nó scale ngang vì công việc được phân vùng. Chi phí thật không phải CPU, mà là state: bộ nhớ gom theo user và khối lượng ghi Postgres. Đó là chỗ em nhìn đầu tiên, không phải số lượng consumer.

**Nếu khối lượng Sui tăng 10x qua đêm, thứ đầu tiên sập là gì?**

Consumer scale ra được, nên điểm áp lực đầu tiên là phía sau — throughput upsert Postgres và cái map gom-theo-user trong bộ nhớ bị 'nóng' với các ví cực kỳ active. Em sẽ shard/tune đường ghi và theo dõi consumer lag trước khi bản thân phần compute trở thành giới hạn.

**Một microservice cho mỗi chuỗi có tốt hơn một service xử lý tất cả không?**

Một service xử lý tất cả các chuỗi qua adapter là đúng cho một team nhỏ — một thứ để vận hành, chung logic lõi. Microservice cho mỗi chuỗi sẽ nhân bội bề mặt deploy/ops cho một team một-tới-vài người. Nếu khối lượng của một chuỗi lấn át phần còn lại, em sẽ tách *chuỗi đó* ra, chứ không shard mặc định.

**Có thể dùng framework stream-processing (Flink/Kafka Streams) thay cho consumer Go tự viết không?**

Em có cân nhắc. Với quy mô và team của bọn em, consumer Go tự viết đơn giản để vận hành và debug hơn là dựng Flink, và logic của bọn em (recompute gom theo user + upsert) không phải một job windowed-streaming tự nhiên. Nếu aggregation phức tạp hơn nhiều, Kafka Streams/Flink mới đáng cái trọng lượng vận hành.

**Vì sao Kafka mà không phải queue như SQS/RabbitMQ hay chỉ poll chuỗi?**

Kafka cho bọn em parallelism theo partition, retention/replay, và back-pressure — em dùng cả ba một cách chủ động (replay lúc cutover, lag làm tín hiệu sức khoẻ). Poll chuỗi trực tiếp thì bó compute vào tình trạng sẵn sàng của RPC; một queue thường không cho em replay hay partition có thứ tự.

**Nếu làm lại từ đầu, bạn sẽ thay đổi gì về các đường ranh giới?**

Em sẽ chuẩn hoá schema event từ ngày đầu và version nó — em có tiến hoá nó hơi tuỳ hứng. Và em sẽ tách state aggregation theo user thành một thứ tường minh hơn sớm hơn, vì đó là phần sau này cần chăm sóc nhiều nhất khi tải cao.

---

## Diagnosed and resolved a critical Kafka consumer-lag incident

Bọn em có lần consumer lag khoảng 4 triệu message trong 3–4 tiếng — ước throughput khoảng 300–400 msg/s nhưng consumer không đuổi kịp. Root cause: 1 message – 1 round-trip DB – 1 commit. Em redesign sang batch; đề xuất ingestor tăng kích thước message (1 tx → n tx / message). Consumer batch max ~1000. Lag steady-state từ hàng triệu xuống vài trăm / vài nghìn; throughput ~10x.

Quy trình: Monitor (Grafana/Prometheus/Kafka-UI) → chẩn đoán (lag ↑ + CPU không bão hoà = I/O-bound) → fix batch → verify lag ↓. Không kể "restart rồi hết" trước.

Trade-off batch: đổi latency per-message nhỏ lấy khả năng đuổi kịp. Quá nhỏ → overhead; quá lớn → memory + retry đắt. Không chỉ thêm consumer: gần số partition rồi; scale ngang không xoá chi phí O(1)/message.

### Follow-up

**Bạn để ý ra sự cố lần đầu thế nào?**

Monitoring/alert lag trên consumer group — metric lag leo đơn điệu thay vì dao động quanh 0, đó là dấu hiệu tốc độ consume < tốc độ produce.

**Các nguyên nhân khả dĩ của lag tăng là gì và bạn thu hẹp thế nào?**

Ba nhóm: produce spike, consumer chậm, hoặc downstream chậm. Em loại produce spike thuần vì nó không hồi phục, và em thấy CPU consumer không bão hoà trong khi lag tăng — cái đó chỉ vào overhead per-message I/O-bound, tức đang chờ Postgres và commit, không phải compute.

**Vì sao xử lý per-message giới hạn throughput của bạn?**

Mỗi message trả một chi phí cố định — một round-trip DB và một commit offset. Chi phí cố định per-message đó đặt trần message/giây bất kể kích thước message. Một khi produce vượt trần đó, lag tăng vô hạn; bạn không thể scale-ra vượt một chi phí per-item vốn quá cao về mặt cấu trúc.

**Thiết kế batched thay đổi cụ thể những gì (DB, commit, dedup)?**

Poll N message, dedup và gom trong bộ nhớ, ghi bulk/transactional vào Postgres, và commit một offset cho mỗi batch. Nên round-trip DB và commit đi từ per-message xuống per-batch — chi phí chủ đạo tụt gần bằng batch factor.

**Bạn chọn batch size thế nào?**

Bằng thực nghiệm — đủ lớn để phân bổ đều chi phí DB và làm phẳng lag, đủ nhỏ để giới hạn bộ nhớ và giữ latency per-batch và chi phí retry hợp lý. Em tune bằng cách theo dõi lag và latency batch thay vì chọn một con số ma thuật.

**Bạn giữ PnL đúng thế nào khi batch (dedup, thứ tự)?**

Dedup trong batch theo event id để gửi lại không đếm hai lần, gom theo user và áp theo thứ tự timestamp, và upsert idempotent theo khoá (ví, token). Batching thực ra làm tính đúng đắn dễ hơn vì em có thể dedup cả một batch trước khi đụng DB.

**Trong sự cố, dữ liệu bị mất hay chỉ bị trễ? Làm sao bạn biết?**

Trễ chứ không mất. Kafka giữ lại mọi thứ và offset chỉ tiến sau khi ghi thành công, nên 4M vẫn nằm trên topic — fix giúp consumer đuổi kịp bằng cách drain chúng, và idempotency nghĩa là xử lý lại an toàn.

**9. Nếu batch hỏng giữa chừng — ghi một phần thì sao?**

Phép ghi batch là transactional và offset chỉ commit sau khi nó thành công, nên hỏng giữa batch sẽ rollback và cả batch xử lý lại. Upsert idempotent + dedup làm việc xử lý lại đó an toàn — không đếm trùng một phần.

**10. Bạn drain 4M backlog an toàn thế nào mà không gây sự cố thứ hai?**

Deploy consumer batched và để nó consume nhanh hơn produce — lag drain đơn điệu. Em theo dõi để nó không gây lỗi khi drain; vì phép ghi idempotent nên em có thể để nó chạy hết công suất mà không sợ hỏng dữ liệu.

**11. Batching thêm latency mỗi message — trade-off đó có chấp nhận được không? Vì sao?**

Có — bọn em đổi một chút latency per-message để giữ trạng thái đuổi kịp. Với một pipeline dữ liệu/analytics, tươi hơn vài trăm ms mỗi message là vô nghĩa nếu bạn đang tụt hàng triệu. Latency có giới hạn, dự đoán được thắng lag vô hạn.

**12. Vì sao không chỉ thêm partition/consumer?**

Thêm partition/consumer không loại bỏ chi phí DB per-message — mỗi consumer vẫn trả nó, và bọn em đã gần số partition. Scale ra một thiết kế kém hiệu quả về cấu trúc chỉ trải rộng sự kém hiệu quả; em sửa hằng số nhân thay vào đó.

**13. Bạn ngăn tái diễn thế nào — alert, autoscaling, load test?**

Alert lag với ngưỡng và xu hướng thật, cộng với thiết kế batched cho headroom để spike bình thường không tới gần trần. Load-test consumer với tốc độ produce cao hơn để bọn em biết trần mới trước khi prod tự tìm ra cho bọn em.

**14. Kế hoạch rollback nếu consumer batched cư xử sai trên prod là gì?**

Consumer batched có thể rollback về image trước, và vì offset chỉ tiến khi thành công và sự kiện được giữ lại, rollback không mất dữ liệu — tệ nhất là xử lý lại. Idempotency là thứ làm rollback an toàn.

**16. Ở 10x tốc độ produce, batching có còn giữ không? Cái gì hỏng tiếp?**

Batching mua một bội số lớn headroom, nên 10x hấp thụ được tới điểm throughput upsert Postgres hoặc bộ nhớ gom-theo-user bão hoà — đó thành giới hạn tiếp theo, và em sẽ scale đường ghi (bulk upsert/sharding) ở đó.

**18. Buffer phía consumer + ghi DB async có được không?**

Buffer async giúp nhưng thêm failure mode riêng — bạn có thể mất buffer khi crash và giờ bạn cần durability cho nó. Batching với commit-sau-khi-lưu cho cùng sự phân bổ đều với chính Kafka làm buffer bền, đơn giản và an toàn hơn.

**19. Bạn có thể dùng đường bulk-load (COPY) thay vì upsert dưới áp lực sự cố không?**

COPY nhanh hơn cho insert thuần, nhưng bọn em cần ngữ nghĩa upsert (conflict-merge) cho tính đúng đắn, nên bulk upsert hợp hơn. Dưới áp lực sự cố em ưu tiên một fix đúng-theo-thiết-kế hơn là đường ghi nhanh tuyệt đối.

**20. Nếu bạn có Kafka Streams, sự cố này có xảy ra không?**

Có thể ít khả năng hơn — Streams batch và quản state cho bạn — nhưng cùng root cause (một sink quá chậm mỗi item) vẫn có thể cắn bạn. Framework sẽ không loại bỏ nhu cầu nghĩ về write amortization; nó chỉ giấu đi cho tới khi không giấu được nữa.

---

## Ensured per-user PnL correctness across ∼20–25 Kafka partitions.

Số partition ~20–25 (ops set cho throughput). Partition **không** key theo user → không có ordering per-user từ Kafka. Phía consumer: gom batch theo user/symbol, áp theo **timestamp-ms**, chống duplicate bằng `INSERT … ON CONFLICT DO NOTHING`. PnL **recompute** từ balance-change (không cộng dồn mù) → reprocess hội tụ cùng số.

Ideally redesign: partition key = userId phía producer. Ownership: ops chọn số partition; mình own correctness phía consume.

### Follow-up

**2. Một sự kiện balance-change là gì và đến từ đâu?**

Bất cứ khi nào holdings của một ví thay đổi do giao dịch, indexer phát một sự kiện balance-change — ví, token, số lượng, chiều, timestamp — lên Kafka. Pipeline của em là consumer của những cái đó; em không đọc chuỗi trực tiếp.

**4. Vì sao consumer batched thay vì per-message?**

Per-message nghĩa là một round-trip Postgres và một commit offset cho mỗi sự kiện, mà ở khối lượng của bọn em thì consumer không theo kịp — đó chính là thứ gây ra sự cố lag. Batching phân bổ đều chi phí ghi DB và cho em dedup và gom trước khi đụng DB.

**5. Idempotency key của bạn là gì, và vì sao chọn cái đó?**

Khoá tự nhiên là (ví, token) cho hàng đã materialize, và per-event là id duy nhất của sự kiện (kiểu tx hash + log index). Event id cho em dedup; khoá (ví, token) cho em mục tiêu upsert idempotent.

**6. Dedup trong batch thực sự hoạt động thế nào ở mức code?**

Em xây một map key theo event id khi ingest batch, nên một sự kiện gửi lại có cùng id chỉ ghi đè/gộp — batch tới được compute có nhiều nhất một bản của mỗi sự kiện. Rồi em gom tập đã dedup theo user để sắp thứ tự.

**7. Bạn xử lý conflict duplicate-key khi upsert thế nào — merge hay ignore?**

Là merge, không phải ignore mù. Nhánh ON CONFLICT của upsert tính lại/gộp hàng thay vì bỏ phép ghi, vì batch mới có thể mang state mới hơn. Ignore sẽ có nguy cơ mất một cập nhật hợp lệ dùng chung khoá.

**8. Bạn commit offset chính xác lúc nào, và vì sao thủ công?**

Sau khi transaction Postgres commit thành công. Commit thủ công là cả điểm mấu chốt — auto-commit có thể đẩy offset trước khi phép ghi của em xuống DB, và như vậy sẽ âm thầm mất dữ liệu khi crash. Commit-sau-khi-lưu cho em at-least-once mà không mất dữ liệu.

**9. Tính lại PnL từ state nào — toàn bộ lịch sử, snapshot + delta, hay running total?**

Là phép tính lại từ balance state chứ không phải cộng dồn mù — điều đó có chủ đích, vì recompute vốn idempotent. Áp cùng một tập balance-change hai lần hội tụ về cùng kết quả, còn increment cộng dồn thì sẽ nhân đôi.

**10. Consumer crash sau khi ghi Postgres nhưng trước khi commit offset — chuyện gì xảy ra?**

Đó là trường hợp an toàn: offset chưa được đẩy, nên khi khởi động lại em xử lý lại batch đó. Vì phép ghi là upsert idempotent và sự kiện đã dedup, xử lý lại cho ra cùng kết quả — không đếm trùng. Đó chính là lý do commit đứng cuối.

**11. Consumer crash sau khi commit nhưng trước khi ghi — chuyện gì xảy ra?**

Điều đó không thể xảy ra với thứ tự của em — em không bao giờ commit trước khi ghi. Nếu bằng cách nào đó nó xảy ra, em mất một batch, nên bất biến (invariant) là ghi-rồi-mới-commit một cách nghiêm ngặt. Thứ tự của hai thao tác này là toàn bộ đảm bảo an toàn.

**12. Hai consumer xử lý sự kiện của cùng một user từ các partition khác nhau đồng thời — race?**

Chúng không thể làm hỏng nhau vì phép ghi là upsert idempotent theo khoá (ví, token) và compute là recompute, không phải increment. Tệ nhất là một phép ghi thừa; merge ON CONFLICT giải quyết. Em làm tính đúng đắn độc lập với concurrency thay vì dựa vào lock.

**13. Một sự kiện đến trễ với timestamp cũ hơn sau khi bạn đã tính xong — giờ sao?**

Vì PnL là recompute từ balance-change chứ không phải một running sum có thứ tự, một sự kiện trễ chỉ kích hoạt một recompute bao gồm nó. Trong một batch em sort theo timestamp; xuyên batch thì mô hình recompute-từ-state nghĩa là thứ tự đến không làm hỏng con số cuối.

**14. Vì sao không dùng exactly-once (Kafka transactions) thay vì upsert idempotent?**

Kafka transactions/exactly-once thêm overhead thật và ràng bạn vào các config producer/consumer cụ thể, mà vẫn không giúp gì cho bài toán thứ tự xuyên partition. Upsert idempotent + commit thủ công cho em effectively-once ở sink với ít phức tạp hơn nhiều — DB là source of truth, nên hãy làm phép ghi an toàn.

**15. Trade-off batch size — quá lớn hay quá nhỏ thì sao?**

Quá nhỏ thì quay lại overhead per-message — round-trip DB lấn át và lag tăng. Quá lớn thì tăng áp lực bộ nhớ, latency mỗi batch, và chi phí xử lý lại khi hỏng. Em tune để giữ lag phẳng mà batch không lớn tới mức retry đắt.

**16. Upsert Postgres thành nút thắt khi tải cao — chẩn đoán và sửa thế nào?**

Đầu tiên xác nhận đó là sink qua việc consumer lag tăng trong khi CPU rảnh — dấu hiệu kinh điển 'đang chờ DB'. Rồi giảm write amplification: batch hiệu quả lớn hơn, bulk upsert, ít round-trip hơn, và tune index/lock trên khoá conflict. Nếu một bảng bị nóng, partition hoặc shard đường ghi.

**17. Bạn phát hiện PnL sai cho một nhóm user tuần trước — hồi phục thế nào?**

Vì Kafka giữ lại sự kiện và phép ghi của em idempotent, hồi phục là replay: reset consumer về trước khoảng bị ảnh hưởng và xử lý lại. Mô hình recompute nghĩa là replay không thể đếm trùng — chính cái replay-safety đó là lý do chính em thiết kế theo cách này.

**18. Traffic tăng 10x — pipeline này hỏng ở đâu trước và bạn làm gì?**

Consumer scale ngang theo partition, nên thứ hỏng đầu tiên là throughput upsert Postgres và cái map gom-theo-user trong bộ nhớ cho các ví nóng. Em sẽ scale đường ghi (bulk upsert, shard bảng nóng) trước khi thêm consumer, vì consumer không phải nút thắt.

**19. Một ví cá voi tạo ra 100x sự kiện so với người khác — bài toán hot-key — xử lý thế nào?**

Đó là bài toán hot-key. Batching đã giúp vì sự kiện của một cá voi gộp thành ít recompute hơn mỗi batch. Nếu một ví vẫn lấn át một partition, lựa chọn là chia nhỏ công việc của nó hoặc chấp nhận latency hơi cao hơn cho key đó — nhưng tính đúng đắn vẫn giữ vì nó vẫn là recompute idempotent theo khoá.

**20. Bạn có dùng Kafka Streams / một stateful stream processor với changelog topic không? Trade-off?**

Kafka Streams với state store có changelog là một lựa chọn hợp lệ — bạn có local state chịu lỗi cho aggregation theo user. Em chọn consumer Go tự viết + Postgres vì Postgres đã là source of truth và em muốn kết quả materialize query trực tiếp được; Streams sẽ thêm một hệ state thứ hai phải vận hành. Nếu logic aggregation lớn lên, em sẽ xem lại.

---

## Built the PnL/profit recompute consumer

Consumer money-critical. PnL/profit tính riêng trước insert; phase làm sạch (overflow, invalid address/tx hash…). Ghi idempotent + dedup vì Kafka at-least-once.

Pipeline một batch: poll → validate/DLQ → dedup theo event id → gom ví/token → recompute → upsert → commit offset.

---

## Designed a per-user, preference-driven price-alert state machine

Price alert theo **state machine** — tránh spam/miss khi giá dao động quanh ngưỡng. Real-time qua websocket (Socket.IO). Preference theo user.

- Ngây thơ `price > threshold` mỗi tick → spam
- Edge-triggered: state TRÊN/DƯỚI, chỉ fire khi cắt ngưỡng
- State in-memory + định nghĩa bền trong DB; nhiều pod → Redis pub/sub; state tái suy ra được khi restart
- Trending anh em: Redis sorted set cửa sổ 5m/1h/6h/1d

### Follow-up

**1. Tính năng cảnh báo giá làm gì từ góc nhìn người dùng?**

Người dùng đặt một quy tắc — 'báo tôi khi TOKEN lên/xuống X' — và nhận một push real-time đúng khoảnh khắc nó thực sự cắt, không phải liên tục khi nó dao động.

**2. Vì sao state machine thay vì check price > threshold mỗi lần?**

Vì 'price > threshold' là level-triggered — nó đúng ở mọi tick trên đường, nên bạn sẽ spam. Một state machine là edge-triggered: nó chỉ bắn ở chuyển từ dưới lên trên, đúng cái người dùng thực sự hiểu là 'đã cắt'.

**3. Vì sao giữ state cảnh báo trong bộ nhớ thay vì thẳng trong Redis/Mongo?**

Latency. Cảnh báo được đánh giá trên mỗi tick giá, và một lần đọc DB mỗi tick mỗi cảnh báo sẽ giết throughput. State trong bộ nhớ biến đường nóng thành một phép so sánh, không phải I/O — Mongo giữ định nghĩa bền, bộ nhớ giữ state sống.

**4. Redis pub/sub điều phối chính xác cái gì ở đây?**

Tính nhất quán qua các pod. Bất kỳ pod nào cũng có thể nhận một tick giá cho trước, nên pub/sub broadcast các chuyển state/cập nhật giá để mọi pod đánh giá việc cắt dựa trên cùng góc nhìn — nếu không hai pod bất đồng về trên/dưới và bắn không nhất quán.

**5. Vì sao deliver qua cả Socket.IO lẫn Kafka?**

Mục đích khác nhau. Socket.IO là push trực tiếp tới người dùng đang kết nối; Kafka để sự kiện cảnh-báo-đã-bắn có sẵn cho các service khác (analytics, thông báo khác) thay vì bị kẹt trong tầng websocket. Tách rời 'sự kiện' khỏi 'push'.

**6. State machine cấu trúc thế nào — state, transition, trigger?**

Hai state mỗi cảnh báo — TRÊN và DƯỚI — với trigger là một tick giá. Chuyển DƯỚI→TRÊN (hoặc ngược lại) phát một sự kiện bắn; ở lại cùng state thì không phát gì. Đó là toàn bộ edge-trigger.

**7. Bạn duy trì các cửa sổ 5m/1h/6h/1d với sorted set thế nào?**

Mỗi cửa sổ là một sorted set score theo hoạt động trong khoảng đó; em thêm/tăng score khi sự kiện tới và dùng ranked range query cho top-N. Các cửa sổ được duy trì bằng cách scope/hết hạn entry theo khoảng thời gian của nó.

**8. Bạn giữ sorted set khỏi phình vô hạn thế nào (hết hạn/trim)?**

Trim/hết hạn — entry cũ rơi khỏi cửa sổ nên set chỉ phản ánh khoảng đó (ví dụ 5 phút gần nhất). Không có nó set phình mãi và 'trending' hết nghĩa 'gần đây'. Nên mỗi cửa sổ bị giới hạn bởi scope thời gian của nó.

**9. Một pod restart và mất state trong bộ nhớ — chuyện gì xảy ra với cảnh báo?**

Nó rebuild — định nghĩa bền nằm trong Mongo và giá hiện tại biết được, nên khi restart pod tái suy ra state trên/dưới của mỗi cảnh báo từ giá hiện tại trước khi bắt đầu bắn. Nên restart không bắn cảnh báo giả.

**10. Redis pub/sub là fire-and-forget — nếu một pod bỏ lỡ một message thì sao?**

Việc pub/sub lossy chính là lý do state tái suy ra được — một message bị lỡ không thể làm desync vĩnh viễn vì state được đối chiếu lại từ định nghĩa bền + giá hiện tại. Em coi pub/sub là một optimization cho độ tươi, không phải source of truth.

**11. Cảnh báo bắn trùng — làm sao tránh báo cho một user hai lần?**

Edge-trigger đã ngăn bắn lặp khi điều kiện còn đúng, và em dedup ở phía delivery theo khoá (cảnh báo, sự kiện cắt) để dù Kafka at-least-once thì một user cũng không bị báo hai lần cho cùng một lần cắt.

**12. In-memory + pub/sub vs một shared Redis state duy nhất — trade-off?**

Một shared Redis state duy nhất đơn giản hơn và luôn nhất quán nhưng đặt một round-trip Redis lên đường nóng mỗi tick. In-memory + pub/sub giữ đường nóng cục bộ để có latency và chỉ dùng pub/sub để sync — bạn đổi một chút phức tạp lấy chi phí per-tick thấp hơn nhiều.

**13. Sorted set vs một time-series DB cho trending — vì sao sorted set?**

TSDB tuyệt cho query lịch sử, nhưng 'top N ngay bây giờ' là một ranking query, và sorted set làm cái đó trong O(log n) một cách native không cần scan. Cho leaderboard sống, sorted set là primitive đúng mục đích; TSDB sẽ là over-engineering query nóng.

**14. Người dùng báo cảnh báo bị lỡ/trễ trên prod — bạn debug thế nào?**

Trace một cảnh báo từ đầu tới cuối: tick giá có tới không, chuyển state có được tính không, pub/sub có lan không, Socket.IO/Kafka có deliver không. Lỡ vs trễ thường tách ở 'việc cắt có được phát hiện không' vs 'delivery có trễ không' — log state machine làm cái đó tách được.

**15. Một glitch price feed gửi một spike rồi correction — làm sao tránh cảnh báo giả?**

State machine giúp vì spike-rồi-correction là hai chuyển; em có thể debounce hoặc yêu cầu việc cắt tồn tại một chút để một glitch một-tick không bắn. Tốt hơn là thêm một xác nhận nhỏ còn hơn báo trên nhiễu.

**16. Hàng triệu cảnh báo active qua nhiều token — state trong bộ nhớ còn scale không?**

In-memory scale tới khi bộ nhớ mỗi pod là giới hạn; rồi bạn shard cảnh báo qua các pod theo token/user để mỗi pod giữ một tập con, và pub/sub chỉ giữ tập con liên quan nhất quán. State mỗi cảnh báo rất nhỏ, nên trần rất cao trước khi cần shard.

**17. Sorted set cho một token nóng phình to — giữ query cửa sổ nhanh thế nào?**

Giữ set giới hạn theo cửa sổ bằng trim, và cho top-N bạn chỉ cần đầu set, vốn giữ O(log n) bất kể tổng kích thước. Nếu tốc độ event thô của một token khổng lồ, aggregate trước khi ghi (bucket theo khoảng) để set không nhận mọi tick.

**18. Redis Streams hay Kafka có tốt hơn pub/sub cho tính nhất quán không?**

Redis Streams/Kafka cho durability và replay mà pub/sub không có — tốt hơn nếu tính nhất quán phải sống sót qua mất message. Em dùng pub/sub có chủ đích vì state tái suy ra được từ store bền, nên em không cần nhất quán bền, chỉ cần nhất quán nhanh. Nếu tái suy ra đắt, em sẽ chuyển sang Streams.

**19. Có thể làm trending bằng một windowed stream processor không? Vì sao/không?**

Được — một windowed stream processor tính trending một cách native. Em chọn sorted set vì chúng kiêm luôn đường đọc low-latency mà UI query trực tiếp; một stream processor vẫn cần chỗ nào đó để serve top-N, mà cái đó là... một sorted set. Nên em giữ nó trong một hệ.

**20. Bạn làm delivery cảnh báo exactly-once tới người dùng thế nào?**

Exactly-once thật sự tới một user là khó vì hop cuối (mạng tới client) luôn có thể hỏng. Thực tế: delivery idempotent theo khoá sự kiện cắt + dedup phía client, để gửi lại vô hại. Em nhắm effectively-once mà người dùng cảm nhận thay vì exactly-once thật sự.

---

# CEX (VDAX — sàn kiểu Binance)

## Giới thiệu

Phần mềm giao dịch crypto giống Binance: user giao dịch bằng tiền mặt trên app thay vì chọn pool/pair như DEX. Em làm **referral reward** và **notification pipeline**.

---

## Implemented a referral-reward Kafka consumer

Referral có API tạo code / relationship. Consumer lấy trade data từ service trading (gRPC), tính reward theo volume, insert DB ở state **Pending**. Consumer cập nhật status theo engine — **chưa hoàn thiện**: chưa chốt nguồn quỹ (pool app vs trích volume), engine chưa có lệnh transfer.

Ship được accrual idempotent (Pending); payout còn prototype. Unique constraint / upsert-ignore chống credit trùng.

### Follow-up


**4. Các sự kiện trade/settlement đến từ đâu và delivery guarantee gì?**

Sự kiện trade/settlement đến từ pipeline sự kiện (kiểu Kafka), vốn at-least-once — nên gửi lại là điều được kỳ vọng và cả engine phải idempotent với nó.

**5. Vì sao bắt buộc dedup bằng ràng buộc DB thay vì trong code ứng dụng?**

Vì check ở tầng ứng dụng bị race — hai handler đồng thời cùng có thể check 'chưa tích luỹ' và cùng insert. Một ràng buộc duy nhất là atomic ở DB; đó là chỗ duy nhất thật sự đảm bảo được once-only, nên tính đúng đắn về tiền nằm ở đó, không phải một check-then-write trong code có thể race.

**6. Idempotency key chính xác của bạn là gì và vì sao (user, transaction)?**

(user, transaction) — người thụ hưởng cộng transaction nguồn. Đó là tính duy nhất tự nhiên: một user được credit nhiều nhất một lần cho một transaction. Nó sống sót qua gửi lại và concurrency vì DB bắt buộc nó.

**7. Bạn tính hoa hồng với decimal thế nào — kiểu dữ liệu, quy tắc làm tròn?**

Kiểu decimal từ đầu tới cuối; hoa hồng là amount × rate bằng decimal với quy tắc làm tròn tường minh (được định nghĩa, không phải hành vi float mặc định). Float sẽ đưa vào sai số biểu diễn cộng dồn qua các tầng và không đối chiếu được tới cent.

**8. Bạn xử lý rollup nhiều tầng thế nào — đệ quy, chuỗi tính trước, hay lặp?**

Em giải chuỗi cho user giao dịch và lặp qua các tầng, phát một accrual cho mỗi tầng với mức của tầng đó. Làm per-tầng thành các bản ghi riêng key theo (user, transaction) nghĩa là mỗi tầng idempotent độc lập.

**9. Hai sự kiện cho cùng một transaction tới đồng thời — chuyện gì xảy ra ở DB?**

Một insert thắng, cái kia đụng ràng buộc duy nhất và bị reject/catch như một bản trùng — mà em coi là thành công (đã tích luỹ), không phải lỗi. Đó chính là lý do ràng buộc tồn tại: nó biến một race thành một no-op.

**10. Một trade sau đó bị đảo/huỷ — bạn claw back hoa hồng thế nào?**

Một lần đảo là một bản ghi bù (compensating), không phải một phép sửa — em tích luỹ một hoa hồng âm/bù key theo transaction đảo. Số dư phải trả suy ra sẽ triệt tiêu chúng, và audit trail giữ cả hai, thứ finance cần.

**11. Insert hoa hồng thành công nhưng một bước downstream hỏng — nhất quán?**

Insert accrual và state liên quan nằm trong một transaction khi có thể, nên nó all-or-nothing. Nếu một bước downstream thật sự tách rời hỏng, bản ghi accrual vẫn là source of truth và downstream có thể retry lại nó một cách idempotent.

**12. Bản ghi accrual bất biến + số dư suy ra vs một running balance có thể sửa — trade-off?**

Bản ghi bất biến + số dư suy ra cho bạn audit trail đầy đủ và làm dedup/claw-back sạch (append các bản bù), đổi lại phải tính số dư. Một running balance có thể sửa thì rẻ để đọc nhưng mất lịch sử và khó đối chiếu hơn nhiều — với tiền em chọn audit trail.

**13. Vì sao decimal thay vì integer-cents (minor units)? Hay bạn dùng minor units?**

Điểm mấu chốt là số học cơ số 10 chính xác với làm tròn định nghĩa được; dù đó là kiểu decimal hay integer minor units, thứ cần tránh là float. Em dùng xử lý chính xác decimal để mức và số lượng nhân chính xác và làm tròn dự đoán được.

**14. Finance báo lệch payout — bạn đối chiếu/audit thế nào?**

Các bản ghi bất biến là audit trail — mỗi payout truy được về (user, transaction, tầng, mức, số tiền). Đối chiếu là tái suy ra số dư từ các bản ghi và so với cái đã trả; lệch khoanh về các bản ghi cụ thể.

**15. Bạn phát hiện một mức bị cấu hình sai suốt một tuần — sửa accrual lịch sử thế nào?**

Vì accrual bất biến và có key, em không viết lại lịch sử — em phát các bản ghi sửa cho các transaction bị ảnh hưởng với delta giữa mức sai và đúng. Sổ vẫn append-only và audit được, và số dư suy ra tự sửa.

**16. Chuỗi giới thiệu rất sâu và viral — rollup còn scale không?**

Độ sâu chuỗi bị giới hạn bởi số tầng (bạn chỉ trả N cấp), nên chi phí rollup mỗi trade là O(tầng), không phải O(độ dài chuỗi) — đó là một cái cap có chủ đích. Nên viral làm tăng số trade, không phải chi phí mỗi trade, và cái đó scale theo consumer.

**17. Khối lượng trade cao — insert unique-constraint có thành hotspot không?**

Insert unique-constraint là một phép ghi có index đơn lẻ; nó rẻ. Dưới khối lượng rất cao, thứ cần theo dõi là index trên (user, transaction), và batch các accrual như đường PnL sẽ phân bổ đều nó — cùng một playbook.

**18. Bạn tính hoa hồng đồng bộ ngay trên trade hay async qua sự kiện? Vì sao?**

Async qua sự kiện. Hoa hồng không nên block hay làm chậm đường trade, và làm nó ngoài stream sự kiện cho em retry/replay và tách các tính năng tăng trưởng khỏi luồng trading quan trọng. Trade hoàn tất; hoa hồng tích luỹ sau, một cách idempotent.

**19. Event-sourcing sổ hoa hồng vs thiết kế hiện tại — có đáng không?**

Thiết kế đã là event-sourcing-lite rồi — các bản ghi accrual bất biến là cái sổ. Event-sourcing đầy đủ (log sự kiện replay được là source of truth duy nhất) sẽ thêm sức mạnh cho audit/replay nhưng thêm máy móc; với cái này thì sổ bản-ghi-có-key trúng điểm ngọt.

**20. Bạn hỗ trợ thay đổi quy tắc hoa hồng thế nào mà không làm hỏng accrual quá khứ?**

Quy tắc được version và áp tại thời điểm accrual, và vì bản ghi bất biến với mức lưu trên nó, đổi quy tắc về sau không bao giờ đụng accrual quá khứ. Lịch sử phản ánh quy tắc có hiệu lực lúc đó — đúng cái finance và người dùng kỳ vọng.

---

## Engineered a notification pipeline (Kafka → Firebase) with time-window batching 

Pipeline từ user activity: watchlist, security (password), KYC… Push theo user setting. Preference preload Redis/mem, refresh qua pub/sub khi API đổi. Time-window batching giảm push trùng. Kênh chính: Firebase.

### Follow-up

**1. Pipeline này gửi những loại thông báo nào?**

Chủ yếu là sự kiện tài khoản/tiền — settlement nạp và rút — cộng các sự kiện user khác, deliver dưới dạng email, in-app real-time, và push mobile.

**2. Vì sao gửi thông báo async qua background worker?**

Vì thông báo không được làm chậm hay gây nguy cho đường settlement. Settlement hoàn tất và commit; worker nhặt sự kiện sau đó. Một provider email chập chờn không bao giờ được phép làm trễ một lần rút của ai đó.

**3. Vì sao ba kênh/transport riêng thay vì một?**

Mỗi kênh có latency, failure mode, và provider khác nhau. Transport riêng cô lập chúng — email chậm không thể block in-app, Firebase down không thể block email. Ghép chúng lại sẽ làm cả pipeline chỉ đáng tin bằng kênh tệ nhất.

**4. Vì sao RabbitMQ cho email khi Kafka đã ở đó?**

Kafka là xương sống sự kiện miền; RabbitMQ là work queue per-kênh cho email với ngữ nghĩa retry/DLQ dễ và cô lập provider. Dùng RabbitMQ cho nhánh email giữ cho retry email khỏi replay cả sự kiện Kafka và tách các mối quan tâm.

**5. Vì sao cần Redis đằng sau Socket.IO?**

Socket.IO giữ connection trên các pod cụ thể, nhưng sự kiện có thể được consume bởi bất kỳ worker nào. Redis (adapter/pub-sub của Socket.IO) cho phép một worker deliver tới socket của một user bất kể pod nào đang giữ nó — không có Redis, real-time chỉ chạy nếu bạn tình cờ ở đúng pod.

**6. Bạn làm delivery idempotent qua các kênh chính xác thế nào?**

Delivery được key theo id sự kiện settlement cho từng kênh; trước khi gửi em check/ghi rằng (sự kiện, kênh) đó chưa được deliver. Gửi lại cùng sự kiện thấy nó đã gửi và bỏ qua — nên Kafka at-least-once không thành thông báo at-least-twice.

**7. Idempotency là per-kênh hay global per sự kiện — và vì sao?**

Per-kênh. 'Đã gửi email' và 'đã gửi push' là hai sự thật độc lập — nếu email thành công nhưng push hỏng, em muốn retry chỉ push, không gửi lại email. Một key global sẽ ép all-or-nothing và gây gửi lại các kênh đã thành công.

**8. Bạn theo dõi cái gì đã deliver thế nào — state đó ở đâu?**

Trong một store theo dõi delivery key theo (sự kiện, kênh) — đủ bền để một worker restart thấy các lần deliver trước. Store đó là thứ làm fan-out an toàn dưới retry và crash.

**9. Firebase down một giờ — chuyện gì xảy ra với push?**

Push retry với backoff qua kênh của nó; các kênh khác không bị ảnh hưởng vì chúng tách rời. Khi Firebase hồi phục, các sự kiện push chưa deliver được retry, và idempotency key chặn các cái đã deliver khỏi gửi trùng.

**10. Provider email chấp nhận rồi âm thầm drop — làm sao bạn biết/retry?**

Drop âm thầm là case khó — em dựa vào tín hiệu delivery/ack của provider khi có và coi thiếu xác nhận là retryable, với idempotency chặn gửi trùng nếu thật ra nó đã đi. Nơi provider không cho tín hiệu, em giới hạn retry và log để đối chiếu.

**11. Một worker crash giữa fan-out (email đã gửi, push chưa) — làm sao tránh gửi lại email?**

Bản ghi delivery per-kênh được ghi khi từng kênh thành công, nên khi restart worker thấy email đã xong và chỉ retry push. Cái bookkeeping per-kênh đó chính xác là thứ ngăn gửi lại kênh đã đi.

**12. At-least-once + idempotency vs cố exactly-once — trade-off ở đây?**

Exactly-once thật sự qua các provider bên thứ ba là bất khả — hop provider luôn có thể hỏng sau khi ack. Nên at-least-once + idempotency key là lựa chọn thực tế và đúng: gửi lại vô hại, không trùng mà người dùng cảm nhận được.

**13. Vì sao fan-out trong một worker vs một consumer riêng cho mỗi kênh?**

Consumer riêng cho mỗi kênh cũng hợp lệ và cho scale độc lập; em fan-out trong một worker với idempotency per-kênh cho đơn giản việc điều phối 'một sự kiện, ba lần deliver'. Nếu một kênh cần scale rất khác, tách nó thành consumer riêng là bước tiếp theo tự nhiên.

**14. Người dùng báo email "rút hoàn tất" trùng — bạn debug thế nào?**

Check store theo dõi delivery cho sự kiện/kênh đó — nếu có hai bản ghi delivery, idempotency key không được áp hoặc key sai; nếu có một nhưng hai email, cái trùng nằm trong provider email/retry RabbitMQ. Cái đó tách nhanh 'bug của mình' khỏi 'bug provider'.

**15. Backlog hình thành trên email queue — bạn xử lý ưu tiên thế nào (tiền vs marketing)?**

Queue/priority riêng — thông báo tiền giao dịch đi đường ưu tiên cao, marketing đường best-effort, nên backlog marketing không bao giờ làm trễ một xác nhận rút. Các loại thông báo khác nhau được các đảm bảo khác nhau.

**16. Khối lượng thông báo tăng 10x trong một sự kiện thị trường — nó hỏng ở đâu?**

Queue hấp thụ spike (đó là việc của nó), nên giới hạn thật đầu tiên là throughput/rate limit của provider và capacity connection Socket.IO. Em sẽ back-pressure qua queue và scale worker/connection; thông báo tiền được ưu tiên để chúng vẫn kịp thời.

**17. Hàng triệu connection Socket.IO đồng thời — delivery có Redis đằng sau scale thế nào?**

Socket.IO scale ngang với adapter Redis phân phối message qua các pod; giới hạn thành throughput pub-sub Redis và số connection mỗi pod. Bạn shard connection qua nhiều pod hơn và có thể partition fan-out Redis nếu nó nóng.

**18. Bạn có dùng Kafka cho mọi kênh (bỏ RabbitMQ) không? Vì sao/không?**

Có thể, nhưng ngữ nghĩa work-queue per-message của RabbitMQ (ack/nack, DLQ, retry) hợp hình dạng 'làm việc này, retry nếu hỏng' của email hơn mô hình log của Kafka. Em giữ Kafka cho sự kiện miền và RabbitMQ cho task queue email vì mỗi tool hợp việc của nó.

**19. Outbox pattern để đảm bảo sự kiện đã được publish — bạn có thêm không?**

Có — một outbox sẽ làm 'settlement đã xảy ra' và 'sự kiện thông báo đã được publish' atomic với DB commit, đóng cái khe nơi một settlement commit nhưng sự kiện không bao giờ publish. Đó là phần hardening đúng nếu bọn em thấy thông báo bị mất; một bổ sung sạch cho thiết kế hiện tại.

**20. Bạn hỗ trợ preference thông báo của user (opt-out per kênh) một cách sạch thế nào?**

Preference như một check trước mỗi lần dispatch kênh — worker tham vấn opt-in per-kênh của user trước khi gửi, nên một opt-out đơn giản là bỏ qua delivery của kênh đó (và bản ghi idempotency của nó). Giữ logic preference ra khỏi bản thân các transport kênh.


---

# Japanese-Learning Platform 

## Giới thiệu

hexagonal architecture, search engine C++ thay vì bật Elasticsearch ngay.

---

## Designed and built a Japanese-learning application on a hexagonal (ports & adapters) architecture in Go/Gin

Backend Go + Gin + Mongo. Domain ở giữa, ports là interface, adapters ở rìa (Mongo, gRPC search, OAuth2/JWT). Composition-root DI trong `main`. Nhiều binary dùng chung core: API, importer, indexer.

### Follow-up

**1. Kiến trúc hexagonal theo lời của bạn là gì?**

Logic miền nằm ở tâm và định nghĩa interface cho cái nó cần; công nghệ cụ thể (DB, web framework, RPC) nằm ở rìa và cắm vào các interface đó. Dependency trỏ vào trong — lõi không bao giờ phụ thuộc ra ngoài.

**2. Vì sao bận tâm với nó trên một dự án cá nhân?**

Vì nó làm lõi testable và cho em đổi implementation mà không đụng logic nghiệp vụ — thứ em thật sự đã làm khi đổi search backend. Kể cả một mình, cái đó trả cổ tức ngay lần đầu bạn đổi ý về một dependency.

**3. Các ports thực tế trong app của bạn là gì, và adapter của chúng?**

Các ports như một vocabulary repository, một search client, một auth provider — mỗi cái là một interface miền sở hữu. Adapter là Mongo repo, gRPC search client, implementation OAuth2/JWT. Miền chỉ thấy các interface.

**4. Vì sao chia thành ba binary thay vì một?**

Chúng có vòng đời khác nhau: API phục vụ request, importer là một batch job nạp dữ liệu, indexer feed search engine. Binary riêng cho phép mỗi cái chạy và scale theo lịch riêng trong khi dùng chung cùng lõi miền.

**5. Composition root là gì và vì sao không DI container?**

Composition root là chỗ duy nhất — trong main của mỗi binary — nơi em construct các adapter cụ thể và inject chúng vào miền. Không container vì wiring tường minh trong Go dễ đọc và được compile check; một container thêm ma thuật runtime mà em không cần ở kích thước này.

**6. Domain error map sang HTTP response thế nào mà lõi không biết HTTP?**

Miền trả các domain error có kiểu — not-found, validation, conflict. Một tầng dịch ở rìa HTTP map chúng sang status code. Nên lõi diễn đạt *cái gì* sai về mặt ngữ nghĩa, và chỉ adapter biết not-found nghĩa là 404.

**7. Bạn cấu trúc middleware validate JWT thế nào và trong token có gì?**

Middleware validate chữ ký và hết hạn của JWT, trích identity/claims, và đặt vào context của request; handler đọc identity từ context, không bao giờ tự parse token. Token mang identity và hết hạn, giữ tối thiểu.

**8. Luồng OAuth2 hoạt động từ đầu tới cuối thế nào ở đây?**

Redirect sang Google, user consent, Google trả một code, em đổi nó phía server lấy identity của user, rồi phát JWT của riêng em cho các request sau. Nên Google lo authentication; JWT của em lo state session sau đó.

**9. Một adapter downstream (Mongo/gRPC) hỏng — miền biểu lộ nó thế nào?**

Adapter trả một error, miền wrap nó thành một domain-level error (ví dụ dependency-failure hay not-found), và rìa map cái đó sang response đúng. Lõi phản ứng với *ngữ nghĩa* hỏng, không phải chi tiết Mongo/gRPC.

**10. Một JWT bị đánh cắp — bạn giới hạn thiệt hại thế nào?**

JWT là stateless nên JWT thuần không thu hồi tức thì được — em giữ hết hạn ngắn để giới hạn cửa sổ, và để thu hồi thật bạn thêm một check phía server (denylist/vô hiệu refresh-token). Access ngắn hạn + refresh là mitigation thực tế.

**11. Hexagonal thêm indirection/boilerplate — khi nào nó không đáng?**

Khi app thật sự là một tầng CRUD mỏng không có logic miền thật, indirection ports/adapters là overhead không lợi ích. Hexagonal trả cổ tức khi có logic miền đáng kể cần bảo vệ và các dependency bạn có thể đổi — nếu không thì là nghi thức.

**12. JWT vs session phía server — vì sao bạn chọn JWT ở đây?**

JWT cho validate stateless — không lookup session store mỗi request, hợp một API stateless nhỏ. Cái giá là thu hồi khó hơn, cái em chấp nhận cho một app cá nhân bằng hết hạn ngắn. Cho một ngân hàng em sẽ cân nhắc lại về phía session phía server.

**13. Bạn cần đổi MongoDB sang Postgres — bao nhiêu thứ thay đổi?**

Lý tưởng chỉ là repository adapter — implement cùng repository port với Postgres và cắm nó vào composition root. Miền và các tầng khác không thay đổi. Đó là cả điểm của port; em tiến gần 'chỉ adapter' tới đâu là bài test xem ranh giới của em có thành thật không.

**14. Một bug chỉ hiện ở binary indexer — thiết kế lõi-dùng-chung giúp debug thế nào?**

Vì lõi dùng chung, em có thể tái hiện hành vi miền trong một test với fake và xác nhận bug nằm trong adapter/wiring của indexer, không phải logic dùng chung. Lõi dùng chung thu hẹp 'nó ở đâu' nhanh — nếu API chạy mà indexer không, thì là adapter.

**15. Nếu nó lớn lên thành một team 10 người, kiến trúc này giúp hay hại onboarding?**

Giúp — người mới học miền ở tâm mà không cần hiểu mọi rìa, và composition root tường minh là một bản đồ dễ đọc về cách mọi thứ nối. Rủi ro là indirection làm rối người mới với pattern, nên cần một README ngắn.

**16. Ba binary cần scale độc lập — thiết kế có hỗ trợ không?**

Có — binary riêng deploy và scale độc lập, nên API chạy nhiều replica trong khi importer là một job thỉnh thoảng. Dùng chung miền như một library không ghép runtime scaling của chúng.

**17. Bạn có dùng `google/wire` hay fx cho DI ở quy mô lớn hơn không? Trade-off vs thủ công?**

Ở quy mô lớn hơn `wire` cho DI compile-time mà không phải tự viết hết wiring, giúp khi graph lớn. Em chọn thủ công vì graph nhỏ và wiring tường minh dễ đọc nhất; em sẽ dùng wire khi boilerplate constructor vượt sự rõ ràng của nó, không phải trước đó.

**18. Hexagonal vs clean architecture vs chỉ packages-by-feature — vì sao hexagonal?**

Chúng là anh em họ — tất cả đẩy dependency vào trong. Em thích cách hexagonal khung 'ports miền sở hữu, adapter ở rìa' vì nó map sạch lên 'em có thể đổi dependency này'. Packages-by-feature cũng ổn, nhưng hexagonal làm swappability tường minh, đó là mục tiêu.

**19. Bạn thêm một transport thứ hai (gRPC API) song song với Gin HTTP API thế nào?**

Thêm một gRPC adapter như một inbound transport khác gọi cùng các domain service — lõi không thay đổi vì nó không biết về transport. Cả HTTP và gRPC thành các rìa mỏng trên cùng các use case. Đó chính xác là thứ ports làm rẻ.

**20. Bạn test miền cô lập thế nào — fake cái gì và không fake cái gì?**

Em fake các outbound port — repository, search client — bằng implementation in-memory và test các use case miền trực tiếp, không DB không network. Em không fake bản thân miền; điểm là để chạy logic nghiệp vụ thật với các rìa fake.

---

## Engineered a custom full-text search engine in C++ (15K docs/sec indexing, p50 = 0.04ms at 30K qps), integrated via gRPC (protobuf)

Tự viết search: ops gọn + học internals. C++17, BM25, inverted index in-memory. Go ↔ engine qua gRPC/protobuf; indexing client-streaming; search gRPC-first, fallback Mongo regex.

| Metric | Giá trị | Caveat |
|--------|---------|--------|
| Indexing | ~15K docs/sec | synthetic, single run |
| Query p50 | ~0.04 ms | không phải prod SLA |
| Peak concurrency | ~30K qps @ 4 threads | không so tuyệt đối cross-machine |
| Dị thường | 2 threads đôi khi chậm hơn 1 | nghi contention — chưa profile xong |

Cách nói: tin mức độ lớn, không bám số tuyệt đối.

### Follow-up

**1. BM25 là gì và vì sao dùng nó thay vì khớp chuỗi con/regex đơn giản?**

BM25 rank document theo relevance — term frequency, inverse document frequency, và length normalization — nên từ phổ biến ít quan trọng hơn và từ khớp hiếm quan trọng hơn. Regex/chuỗi con chỉ nói *có* một từ xuất hiện không, không phải document *relevant* cỡ nào; BM25 cho bạn kết quả có rank.

**2. Vì sao tự viết engine thay vì Elasticsearch?**

Hai lý do thành thật: giữ một dự án cá nhân gọn nhẹ về vận hành thay vì chạy một cluster Elasticsearch, và như một bài tập systems-programming có chủ đích để thật sự hiểu nội tại search. Không phải 'Elasticsearch dở' — mà là một quyết định build-để-học-và-giữ-gọn có ý thức.

**3. Engine cấu trúc thế nào — inverted index, postings, scoring?**

Một inverted index — term → postings list các document (và term frequency) — cộng metadata document cho length normalization. Một query lookup postings mỗi term và cộng dồn điểm BM25 qua các document khớp, rồi trả top-k.

**4. Vì sao gRPC/protobuf qua ranh giới Go↔C++ thay vì REST hay FFI?**

gRPC/protobuf cho một contract có kiểu, được version, hiệu quả qua một ranh giới process và qua hai ngôn ngữ, với streaming built-in. FFI sẽ ghép chặt các binary và chia sẻ một không gian bộ nhớ em không muốn; REST sẽ chatty hơn và không kiểu. gRPC hợp một service riêng throughput cao nhất.

**5. Vì sao client-streaming cho indexing cụ thể?**

Vì indexing vốn là bulk — một RPC mỗi document nghĩa là overhead per-call lấn át. Client-streaming cho client đẩy cả một batch qua một call, nên engine ingest liên tục; sự phân bổ đều đó là thứ đưa throughput lên tầm ~15K/giây.

**6. Bạn xây và lưu inverted index thế nào — trong bộ nhớ, trên đĩa?**

Chủ yếu một inverted index in-memory cho tốc độ query, xây khi document stream vào. Đó là vì sao query latency tí xíu — nó là lookup bộ nhớ và số học — và cũng là vì sao bộ nhớ/persistence là mối lo scale chính, cái em ý thức được.

**7. Một query được scored từ đầu tới cuối thế nào?**

Tokenize query, lookup postings mỗi term, và với mỗi candidate document cộng dồn đóng góp BM25 của mọi query term nó khớp, rồi giữ một top-k heap. Nên nó là postings intersection/union cộng scoring cộng một top-k selection.

**8. Logic gRPC-first-với-fallback quyết định fallback thế nào?**

Client thử gRPC search; khi error hoặc không sẵn sàng nó fallback về một MongoDB regex query. Đó là một fallback health/error-driven — đường chính là engine nhanh, và fallback đổi relevance và tốc độ lấy availability để search không bao giờ hard-fail.

**9. Engine C++ crash giữa chừng index-stream — bạn còn lại ở state nào?**

Vì nó là một index in-memory xây từ một stream, một crash giữa stream mất index đang dở — đó là vì sao indexing là một job bulk chạy lại được, không phải source of truth. Dữ liệu nguồn nằm trong Mongo, nên em chỉ re-index; engine là một view suy ra, xây lại được.

**10. Engine và Mongo bất đồng kết quả lúc fallback — có phải vấn đề không?**

Nó được kỳ vọng — fallback là khớp chuỗi con regex, nên kém relevant và không rank so với BM25. Đó là một degradation chấp nhận được lúc outage; điểm là 'search vẫn chạy', không phải 'kết quả giống hệt'. Em sẽ không dựa vào chúng khớp nhau.

**11. Build-vs-buy: tự viết tốn bạn cái gì so với Elasticsearch?**

Nó tốn em các tính năng Elasticsearch cho miễn phí — fuzzy, phrase, prefix, analyzer, phân tán, persistence — và gánh nặng tự lo correctness/benchmark. Em mua sự đơn giản vận hành và hiểu biết sâu. Cho một sản phẩm thật với các nhu cầu đó, trade đó lật về phía buy.

**12. Index in-memory (nhanh, dễ bay) vs persistent — bạn chọn cái nào và vì sao?**

In-memory cho tốc độ và đơn giản, chấp nhận dễ bay vì index xây lại được từ Mongo. Trade-off là bộ nhớ giới hạn kích thước corpus và một restart nghĩa là re-index. Cho quy mô lớn hơn em sẽ thêm persistence hoặc segment memory-mapped.

**13. Bạn đo 2 thread chậm hơn 1 — giả thuyết của bạn là gì và bạn xác nhận thế nào?**

Giả thuyết của em là lock contention hoặc oversubscription trong đường indexing đa thread — cách shard-merge có lẽ có overhead đồng bộ lấn át ở 2 thread. Em sẽ xác nhận bằng một lock/CPU profiler, cái em rõ ràng không chạy — nên bây giờ nó là một giả thuyết, không phải một chẩn đoán.

**14. Peak RSS ~211MB — chuyện gì xảy ra khi corpus lớn 100x?**

Ở 100x index in-memory sẽ không vừa thoải mái, nên RSS thành bức tường. Em sẽ chuyển sang lưu trữ segment/memory-mapped hoặc shard qua các process. Thiết kế in-memory một-process hiện tại đúng cho kích thước corpus em test, không phải cho 100x.

**15. Bạn scale nó tới hàng triệu document vượt một process thế nào?**

Shard index qua các process/node theo document, fan một query ra mọi shard, và merge top-k từ mỗi cái. Đó về cơ bản là cái Elasticsearch làm — tại điểm đó em sẽ nghiêm túc cân nhắc dùng một engine có sẵn thay vì tự implement lại phân tán.

**16. Bạn xử lý cập nhật/xoá index, không chỉ bulk load, thế nào?**

Xoá thường là tombstone trong postings với compaction định kỳ; cập nhật là xoá-rồi-reindex document. Đường hiện tại của em thiên về bulk-load, nên cập nhật/xoá incremental đúng là loại tính năng nơi độ chín của một engine trưởng thành bắt đầu thắng.

**17. Bạn thêm phrase/fuzzy/prefix query mà hiện chưa hỗ trợ thế nào?**

Chúng là bổ sung trên inverted index — prefix qua một term dictionary/trie, phrase qua positional postings, fuzzy qua edit-distance trên term dictionary. Em không implement chúng (em liệt chúng là excluded), đó là scope thành thật; mỗi cái là một khối việc thật.

**18. Bạn có shard index không? Query fan-out/merge hoạt động thế nào?**

Shard theo document, query mọi shard song song, merge theo điểm cho top-k toàn cục — length normalization cần cẩn thận để điểm per-shard so sánh được (hoặc bạn gom stat toàn cục). Fan-out/merge là cách chuẩn; đúng global IDF qua các shard là phần tinh tế.

**19. Thành thật, benchmark của bạn đáng tin cỡ nào, và cái gì làm nó đáng tin?**

Thành thật, không mấy — một run mỗi config, corpus tổng hợp, judgment relevance tổng hợp, không profiler, và một dị thường contention em chưa đào. Em tin ở mức độ lớn. Để làm chúng đáng tin em sẽ chạy lặp với confidence interval, một corpus thực tế, relevance gán bởi người, và profiling thật.

**20. Nếu cái này lên production thật, bạn giữ nó hay chuyển sang Elasticsearch/Tantivy? Vì sao?**

Cho một sản phẩm thật em có lẽ sẽ chuyển sang thứ như Elasticsearch hay một library Rust như Tantivy — em sẽ có phân tán, query phong phú hơn, và persistence mà không phải bảo trì một engine. Em chỉ giữ cái của em nếu sự gọn nhẹ vận hành thật sự vượt các cái đó, mà với đa số sản phẩm thì không. Xây nó đáng vì hiểu biết, không nhất thiết cho production.

---

## Load-tested the Go API with k6 (19K req/s health, 4.4K req/s vocabulary, p99 < 32ms, stable to 800 concurrent users)

> Bullet đo hiệu năng API (nếu phỏng vấn hỏi benchmark / capacity).

### Follow-up

**1. Bạn cố học gì từ load testing?**

Giới hạn thật của API — throughput tối đa mỗi endpoint, tail latency, và chỗ nó bão hoà dưới tải thực tế — để quyết định scaling và capacity dựa trên dữ liệu, không phải đoán.

**2. Con số health và vocabulary nói gì cho bạn?**

Health không làm việc thật, nên ~19K là trần runtime/framework. Vocabulary đụng MongoDB, nên ~4.4K là trần việc-thật. Khoảng cách là chi phí round-trip DB và query — nó nói em rằng đường DB là ràng buộc, không phải bản thân Go.

**3. Bạn thiết kế bộ test thế nào (cô lập vs workflow)?**

Hai tầng: test cô lập per-endpoint (VU cố định, không pacing) để tìm capacity thô của mỗi endpoint, và một test workflow ramp concurrency qua hành trình user thật để tìm điểm bão hoà hệ thống. Capacity vs hành-vi-dưới-tải.

**4. Vì sao đo p99/p95 thay vì latency trung bình?**

Vì trung bình giấu tail — một mean tốt vẫn có thể nghĩa là vài user có trải nghiệm tệ. p99 nói em cái mà 1% chậm nhất thấy, đó là thứ thật sự hiện ra thành 'app cảm giác chậm'. Percentile là câu chuyện latency thành thật.

**5. "Điểm bão hoà" nghĩa là gì và bạn tìm nó thế nào?**

Nó là chỗ req/s phẳng lại dù bạn thêm tải, hoặc p99 bắt đầu tăng vọt — hệ thống không làm được nhiều việc hữu ích hơn, nó chỉ queue. Bạn tìm nó bằng cách ramp concurrency và theo dõi cái inflection đó trong khi tương quan với CPU/bộ nhớ.

**6. Vì sao health 19K nhưng vocabulary chỉ 4.4K — thời gian đi đâu?**

Round-trip DB: lấy connection, query Mongo, và deserialize. Health trả ngay; vocabulary trả I/O và việc. Nên khoảng cách ~4x về cơ bản là cái giá của việc đụng database mỗi request.

**7. Bạn tương quan latency với nguyên nhân (CPU vs DB) thế nào?**

Em ghi CPU/bộ nhớ API và Mongo trong lúc ramp, nên khi latency tăng em thấy được liệu CPU API bị đóng đinh (compute-bound) hay Mongo là ràng buộc (DB-bound). Tương quan latency với các cột resource là cách quy nguyên nhân chậm.

**8. Bạn cấu hình gì trong k6 — VU, pacing, ramp?**

Virtual user (concurrency), thời lượng test, có pacing (think-time) hay không, và profile ramp. Test cô lập là no-pacing max-throughput; test workflow ramp VU theo từng stage để vẽ đường cong bão hoà.

**9. Vài endpoint cho error rate 40–50% — đó là gì và có thật không?**

Chúng chủ yếu là artifact test-setup — endpoint cần auth/data mà script không thoả mãn đầy đủ — không phải server đổ. Em gọi ra điều đó thành thật; nó nghĩa là các con số throughput cụ thể đó kém đáng tin, nên em dựa vào các con số sạch.

**10. Chuyện gì xảy ra ngay sau điểm bão hoà — nó hỏng thế nào?**

Vượt bão hoà, throughput plateau và latency leo khi request queue — cuối cùng bạn nhận error từ timeout hoặc cạn connection-pool/CPU. Nó degrade bằng queue rồi error, không phải một vách sạch, đó là vì sao theo dõi p99 bắt nó sớm.

**11. Bạn tin con số chạy một lần cỡ nào? Giới hạn là gì?**

Không mấy khi đứng riêng — một run, một máy, tổng hợp. Em coi chúng là tín hiệu capacity và mức độ lớn, không phải release gate. Để tin chúng em muốn chạy lặp và confidence interval.

**12. Throughput vs latency — bạn suy luận về trade-off thế nào?**

Chúng trade-off sau bão hoà: dưới nó bạn đẩy throughput với latency ổn; tới gần nó, thêm tải mua queue (latency cao hơn) không phải throughput hơn. Điểm vận hành hữu ích là dưới đầu gối nơi latency còn phẳng.

**13. Trong prod, p99 tăng vọt nhưng throughput ổn — bạn điều tra thế nào?**

p99 tăng với throughput phẳng gợi ý nguồn tail — GC pause, một dependency chậm, lock contention, hay chờ connection-pool — chứ không phải quá tải thô. Em sẽ xem GC/trace metric và DB pool wait time, vì mean ổn loại trừ quá tải toàn cục.

**14. Bạn phân biệt vấn đề cạn connection-pool với bão hoà CPU thế nào?**

Cạn pool hiện ra là latency tăng trong khi CPU *không* bão hoà — request chờ một connection rảnh. Bão hoà CPU hiện ra là CPU đóng đinh. Tương quan resource tách 'đang chờ' khỏi 'đang làm', cùng tín hiệu em dùng trong sự cố Kafka.

**15. Bạn cần 5x capacity — dựa trên kết quả này bạn tối ưu gì trước?**

Dựa trên kết quả này, đường DB — nên caching vocabulary query nóng, tune query/index Mongo, và connection pool. Health chứng minh tầng app còn headroom, nên 5x đến từ loại bỏ round-trip DB, không phải tối ưu Go.

**16. Làm sao bạn biết là DB chứ không phải GC hay scheduler của Go ở concurrency cao?**

Vì khi Mongo là ràng buộc, CPU API không đóng đinh và resource metric của Mongo động đậy; nếu là GC/scheduler, em sẽ thấy nó trong Go runtime metric (GC pause, số goroutine) với DB rảnh. Em sẽ xác nhận bằng pprof thay vì giả định.

**17. Bạn có thêm caching cho vocabulary query không? Nó thay đổi gì?**

Caching vocabulary (nó khá tĩnh, dữ liệu tham chiếu) sẽ đẩy endpoint đó về phía trần health bằng cách loại round-trip DB khi cache hit — thắng lớn. Trade-off là cache invalidation, nhưng dữ liệu từ điển thay đổi hiếm, nên là một fit mạnh.

**18. Bạn làm benchmark đáng tin đủ để gate release thế nào?**

Chạy lặp với confidence interval, một corpus và workload đại diện cố định, hardware pin, và một ngưỡng regression — rồi nó gate được. Bây giờ chúng là một-run, nên chúng chẩn đoán, không phải gating; làm chúng gate chủ yếu là về khả năng lặp lại.

**19. k6 vs wrk vs Gatling/JMeter — vì sao k6?**

k6 script bằng JS, có ngữ nghĩa ramp/threshold tốt, và mô hình VU và scenario sạch, hợp cả test cô lập lẫn workflow của em. wrk nhẹ hơn nhưng ít script được; Gatling/JMeter nặng hơn. k6 là fit tốt nhất cho tải có script, theo stage.

**20. Bạn load-test đường gRPC search, không chỉ HTTP, thế nào?**

k6 hỗ trợ gRPC, nên em sẽ script các call streaming/unary tới search service, ramp concurrency, và theo dõi latency percentile và bộ nhớ của engine — cùng methodology, khác protocol, và em sẽ tương quan với resource use của engine C++ để tìm bão hoà của nó.

---

## Spaced-repetition scheduling (SM-2): per-user decks, JLPT-filtered new-word discovery

> Bullet học thuật / product side của Kotoba (nếu hỏi feature học).

### Follow-up

**1. Spaced repetition là gì và vì sao nó giúp học?**

Ôn tài liệu ở các interval tăng dần ngay trước khi bạn sắp quên — nó khai thác spacing effect để bạn nhớ nhiều hơn với ít ôn tổng hơn. Bạn thấy thứ khó thường xuyên và thứ dễ hiếm.

**2. SM-2 làm gì ở mức cao?**

SM-2 theo dõi, per-thẻ, bạn nhớ nó dễ cỡ nào và lập lịch lần ôn tới tương ứng — chấm mức nhớ, nó điều chỉnh một ease factor và kéo dài hoặc thu ngắn interval. Nhớ tốt đẩy lần ôn tới xa hơn; một fail kéo nó về sớm.

**3. Bạn lưu state gì per-thẻ per-user?**

Ease factor, interval hiện tại, số lần lặp, và ngày ôn tới — per-user per-thẻ. Bộ bốn đó là toàn bộ state lập lịch; mọi thứ khác suy ra từ nó.

**4. Bạn query "thẻ nào đến hạn bây giờ" hiệu quả thế nào?**

Là một query trên ngày-ôn-tới <= now cho user đó, được index trên (user, next_review_date). Nên 'đến hạn bây giờ' là một index range scan, không phải một phép tính trên mọi thẻ.

**5. Lọc JLPT chọn từ mới thế nào?**

Khám phá từ mới lọc pool vocabulary theo cấp JLPT của user và loại các thẻ đã có trong deck, nên thẻ mới đúng trình độ và chưa thấy. Nó giữ độ khó khớp với người học.

**6. Dẫn tôi qua cập nhật SM-2 trên một lần ôn (ease factor, interval).**

Khi ôn: nếu nhớ tốt, ease factor được nhích (giới hạn bởi một sàn), và interval mới là interval cũ × ease factor (với vài bước đầu cố định, kiểu 1 rồi 6 ngày); nếu nhớ kém, số lần lặp reset và interval tụt về một giá trị ngắn. Rồi em lưu bền interval, ease, số lần lặp, và ngày ôn tới mới.

**7. Bạn xử lý một thẻ fail/again thế nào — hành vi reset?**

Một fail reset số lần lặp và interval về giá trị ngắn/ban đầu nên thẻ quay lại nhanh, và ease factor bị phạt nên nó sẽ lớn chậm hơn lần sau. Thẻ phải được kiếm lại về các interval dài.

**8. Bạn giới thiệu thẻ mới vs ôn trong một phiên thế nào — tỷ lệ?**

Một phiên trộn ôn đến hạn với một số thẻ mới có cap, nên bạn không bị ngập — ôn là ưu tiên (chúng nhạy thời gian), và thẻ mới lấp phần capacity còn lại tới một giới hạn ngày. Cái cap đó là thứ giữ khối lượng công việc hợp lý.

**9. Một user ôn cùng một thẻ hai lần nhanh (double submit) — lịch có hỏng không?**

Nó không nên — một lần ôn áp một transition kiểu idempotent dựa trên điểm, và một double submit hoặc bị dedup bởi bản ghi ôn hoặc chỉ áp hai lần; để an toàn em key lần ôn để lịch cập nhật một lần mỗi lần ôn có chủ ý. Bảo vệ cập nhật là cách sửa.

**10. Vấn đề clock/timezone — bạn định nghĩa "đến hạn hôm nay" cho user toàn cầu thế nào?**

Định nghĩa 'đến hạn' tương đối với ngày cục bộ của user, lưu timestamp bằng UTC và áp timezone của user cho ranh giới ngày. Nếu không 'đến hạn hôm nay' mơ hồ qua các timezone và thẻ hiện/biến ở các giờ lạ.

**11. SM-2 vs một scheduler hiện đại như FSRS — trade-off?**

SM-2 đơn giản, hiểu rõ, và đủ tốt; FSRS chính xác hơn vì nó mô hình hoá trí nhớ với nhiều tham số hơn và fit theo dữ liệu. Em dùng SM-2 cho đơn giản và vì nó đã được kiểm chứng; FSRS sẽ là bản nâng cấp nếu em muốn độ chính xác data-driven, đổi lại phức tạp.

**12. Hàng per-user-per-thẻ vs tính lịch on-the-fly — trade-off lưu trữ?**

Lưu hàng per-user-per-thẻ làm 'đến hạn' thành một query index rẻ và giữ lịch sử; tính on-the-fly tiết kiệm lưu trữ nhưng làm 'cái gì đến hạn' đắt và khó-stateless. Cho một app ôn thì pattern query chi phối, nên em lưu state.

**13. User than ôn dồn lại sau một kỳ nghỉ — bạn xử lý backlog thế nào?**

Cap tải ôn ngày và/hoặc lập lịch lại backlog để nó không đổ hết một lúc — dồn giết động lực. Bạn trải các thẻ quá hạn thay vì trình hàng trăm thẻ một phiên; nó là một quyết định sản phẩm cũng như lập lịch.

**14. Một bug lập lịch được ship và interval sai — bạn sửa state hiện có thế nào?**

Vì state là hàng per-thẻ, em có thể tính lại hoặc sửa các trường bị ảnh hưởng trong một migration — và vì em lưu số lần lặp và ease, em thường xây lại được ngày ôn tới đúng từ chúng thay vì reset tiến độ của user. Lưu full state là thứ làm cách sửa không-phá-huỷ.

**15. Hàng triệu user × hàng nghìn thẻ — query "đến hạn" còn scale không?**

Có, vì 'đến hạn' là một query index (user, next_review_date), nên nó scale per-user bất kể tổng kích thước — bạn chỉ bao giờ fetch thẻ đến hạn của một user. Index là thứ giữ nó phẳng; không có nó bạn sẽ scan mọi thứ.

**16. Bạn cache hoặc precompute deck đến hạn thế nào để bắt đầu phiên nhanh?**

Precompute hoặc cache deck đến hạn của mỗi user cho ngày hiện tại nên bắt đầu phiên là một lần đọc, refresh khi các lần ôn đến. Vì tính-đến-hạn chỉ thay đổi khi ôn hoặc sang ngày, nó rất cacheable.

**17. Bạn có A/B test tham số scheduler không? Thế nào?**

Có — nhóm user và biến các tham số như điều chỉnh ease hay cap thẻ-mới, rồi so sánh retention và tải ôn. Metric là retention vs công sức; bạn cần đủ user và thời gian để nó có ý nghĩa.

**18. Bạn migrate user từ SM-2 sang một thuật toán tốt hơn thế nào mà không mất tiến độ?**

Map state per-thẻ hiện có (interval, ease, số lần lặp) vào mô hình của thuật toán mới như một warm start thay vì reset — đa số scheduler seed được từ lịch sử ôn. Giữ tiến độ là cả ràng buộc, và state đã lưu làm nó khả thi.

**19. Bạn cá nhân hoá ease/interval vượt SM-2 thuần thế nào?**

Cá nhân hoá điều chỉnh ease hay interval per-user dựa trên độ chính xác quan sát của họ — thực chất fit độ hung hăng theo cách họ thật sự làm, hướng mà FSRS chính thức hoá. SM-2 thuần dùng hằng số cố định; cá nhân hoá làm chúng thích nghi.

**20. Bạn xử lý leech (thẻ user cứ fail hoài) thế nào?**

Phát hiện thẻ fail lặp lại (một ngưỡng leech trên số lapse), rồi flag hoặc suspend chúng để chúng ngừng lấn át ôn, và đưa chúng ra để giúp thêm. Lưu số lần lặp/lapse chính xác là thứ cho phép em phát hiện leech.
