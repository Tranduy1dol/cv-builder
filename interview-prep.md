# Resume → Playbook Kể Chuyện Phỏng Vấn

> Mục đích: biến mỗi gạch đầu dòng trong CV thành những câu chuyện nghe như một kỹ sư đang kể lại công
> việc thật của mình một cách tự nhiên — đáng tin, vững về kỹ thuật. Mỗi câu chuyện được thiết kế để tạo
> ra các *hook* kéo người phỏng vấn về đúng những chủ đề bạn muốn nói.
>
> Cách dùng: đọc to phần **Elevator Pitch** và **Kịch Bản Chốt** cho tới khi nó nghe như *lời của bạn*,
> không phải học thuộc. Giữ phần **Câu Hỏi Đào Sâu + Câu Trả Lời Chuẩn** làm bản đồ trong đầu cho các câu
> hỏi follow-up. Phần **Whiteboard** chỉ cho bạn vẽ gì và vẽ lúc nào.

---

## Sơ đồ chung (RaidenX / VDAX)

Kiến trúc chung mà bạn sẽ quay lại nhiều lần:

```
                    ┌──────────────┐
 on-chain events →  │  ingestors   │ → Kafka topics (~20–25 partitions)
 (indexers)         └──────────────┘        │
                                            ▼
                                   ┌──────────────────┐
                                   │  insight / PnL   │  batched consumers
                                   │  consumers (Go)  │  gom theo user trong consumer
                                   └──────────────────┘
                                       │            │
                             upsert    ▼            ▼   pub/sub
                                 ┌──────────┐   ┌─────────┐
                                 │ Postgres │   │  Redis  │ → sorted sets (rankings)
                                 └──────────┘   └─────────┘ → pub/sub (cross-pod)
                                                     │
                                                     ▼
                                            Socket.IO / Kafka → clients
```

---

# PHẦN 1 — RaidenX (DEX aggregator đa chuỗi & nền tảng dữ liệu giao dịch)

---

## Bullet R1 — Viết lại `insight` từ prototype TypeScript một-chuỗi thành Go event-driven trên 5+ chuỗi (gần như một mình)

> *"Re-architected the `insight` trading-data service from an inherited single-chain TypeScript prototype
> into event-driven Go, then extended it across 5+ chains (Sui, BSC, Base, Solana, Monad) — driving the
> migration largely solo before the team scaled to 8–10 engineers."*

### 1. Ý đồ (Intent)
Đây là tín hiệu về **quyền sở hữu (ownership) + khả năng phán đoán**. Nó nói lên: bạn nhận một thứ dở dang,
lộn xộn, hiểu nó thật sâu, đưa ra một quyết định kiến trúc lớn — rồi thực thi khi thậm chí còn chưa có team
để dựa vào. Không phải "em viết một service"; mà là "em sở hữu một cuộc viết lại mà cả sản phẩm sau này đứng
lên trên đó." Ấn tượng bạn muốn tạo: *người này được tin giao việc mơ hồ, rủi ro cao và không cần cầm tay chỉ việc.*

### 2. Elevator Pitch (20–30 giây)
"Một trong những thứ lớn nhất em own ở RaidenX là service `insight` — đó là backend dữ liệu giao dịch, lo
PnL, rankings, cảnh báo giá, đủ thứ. Lúc em nhận thì nó là một prototype TypeScript một-chuỗi ai đó để lại;
nó chạy được với Sui nhưng sẽ không sống nổi khi bọn em thêm chuỗi hay tăng tải. Nên em viết lại bằng Go
theo hướng event-driven rồi mở rộng lên hơn năm chuỗi. Em làm phần lớn migration đó một mình trước khi team
lớn lên tầm tám tới mười người, nên em được quyết khá nhiều về kiến trúc ban đầu."

### 3. Bản Sâu (1–2 phút)
"Bối cảnh là thế này: RaidenX là một DEX aggregator đa chuỗi, và `insight` là service biến hoạt động giao
dịch on-chain thô thành những thứ người dùng thực sự quan tâm — lãi/lỗ, ROI, token trending, cảnh báo giá.
Lúc em vào, service đó tồn tại dưới dạng một prototype TypeScript chỉ xử lý Sui, và thật lòng nó giống proof-of-concept
hơn là một hệ thống — đồng bộ (synchronous), khó suy luận khi tải cao, và bị bó chặt vào các giả định riêng của Sui.

Vấn đề nằm ở roadmap: bọn em biết chắc sẽ đi đa chuỗi, mà prototype thì không gánh nổi. Mỗi chuỗi mới sẽ là
một lần copy-paste logic và nhân đôi số điểm hỏng. Ràng buộc là em gần như là người backend duy nhất ở giai
đoạn đầu, nên thứ em xây phải là thứ em tự vận hành được một mình và bàn giao sạch sẽ sau này.

Quyết định của em là chuyển sang Go và làm nó event-driven — consume các sự kiện đã chuẩn hoá từ Kafka thay
vì xử lý đồng bộ, và tách 'ingest' khỏi 'compute' khỏi 'serve'. Chọn Go cụ thể vì phần còn lại của stack là
Go, nó tuyệt cho các consumer concurrent throughput cao, và câu chuyện deploy trên Kubernetes đơn giản hơn.
Cái trade-off em chấp nhận là viết lại thì chậm hơn lúc đầu so với vá prototype — nhưng prototype là ngõ cụt,
nên em lập luận rằng nên trả chi phí đó một lần cho xong.

Phần mở rộng chuỗi mới là chỗ thiết kế thật sự: em đẩy toàn bộ thứ đặc thù từng chuỗi (cách đọc một swap,
decimals, định dạng địa chỉ) ra sau một lớp chuẩn hoá (normalization boundary) để logic lõi PnL/ranking không
bao giờ phải biết một sự kiện đến từ chuỗi nào. Đó chính là thứ giúp bọn em đi từ một chuỗi lên Sui, BSC,
Base, Solana, Monad mà logic lõi không phải rẽ nhánh theo từng chuỗi.

Kết quả: nó trở thành xương sống mà team scale lên trên đó — tới lúc bọn em tám tới mười kỹ sư, mọi người
thêm tính năng lên trên nó thay vì phải vật lộn với nó. Bài học lớn nhất với em là vẽ đúng các đường ranh
giới từ sớm; cái normalization boundary là quyết định duy nhất khiến mọi thứ sau đó rẻ đi."

### 4. Interview Hooks
- "prototype TypeScript một-chuỗi … thành Go event-driven" → *Vì sao Go? Vì sao event-driven? Vì sao viết lại thay vì refactor?*
- "mở rộng lên 5+ chuỗi" → *Trừu tượng hoá khác biệt giữa các chuỗi thế nào? Cái gì hỏng theo từng chuỗi?*
- "gần như một mình trước khi team scale" → *Làm sao giữ nó maintainable cho một team chưa tồn tại?*
- "event-driven" → *Nguồn sự kiện là gì? Kafka? Delivery guarantee?*

### 5. Câu Hỏi Đào Sâu (khó dần)
1. (L1) Service `insight` thực chất làm gì?
2. (L1) Vì sao prototype TypeScript cần thay thế thay vì mở rộng?
3. (L2) Vì sao Go thay vì giữ Node/TypeScript?
4. (L2) "Event-driven" ở đây cụ thể nghĩa là gì — nguồn sự kiện và hình dạng của nó?
5. (L2) Bạn cấu trúc service bên trong thế nào (ingest/compute/serve)?
6. (L3) Bạn trừu tượng hoá logic đặc thù chuỗi thế nào để lõi giữ chain-agnostic?
7. (L3) Ở tầng dữ liệu, Sui, một chuỗi EVM, và Solana khác nhau chính xác ở đâu?
8. (L3) Bạn chuẩn hoá decimals/số lượng token qua các chuỗi thế nào mà không bị lỗi độ chính xác?
9. (L4) Một chuỗi mới gửi sự kiện lỗi hoặc sai thứ tự — chuyện gì xảy ra?
10. (L4) Bạn roll out service Go thế nào mà không mất dữ liệu so với TS đang chạy?
11. (L5) Viết lại vs migration kiểu strangler tăng dần — vì sao bạn chọn cái bạn chọn?
12. (L5) Bạn đánh đổi gì khi chọn event-driven thay vì request/response?
13. (L6) Có thứ gì regress sau khi launch một chuỗi trên prod — bạn phát hiện/xử lý thế nào?
14. (L6) Bạn verify số của Go khớp số của TS lúc cutover thế nào?
15. (L7) Thêm chuỗi thứ 6, thứ 10 ảnh hưởng tải và chi phí ra sao? Nút thắt ở đâu?
16. (L7) Nếu khối lượng Sui tăng 10x qua đêm, thứ đầu tiên sập là gì?
17. (L8) Một microservice cho mỗi chuỗi có tốt hơn một service xử lý tất cả không?
18. (L8) Có thể dùng framework stream-processing (Flink/Kafka Streams) thay cho consumer Go tự viết không?
19. (L8) Vì sao Kafka mà không phải queue như SQS/RabbitMQ hay chỉ poll chuỗi?
20. (L8) Nếu làm lại từ đầu, bạn sẽ thay đổi gì về các đường ranh giới?

### 6. Câu Trả Lời Chuẩn
1. "Nó là phía đọc/analytics của nền tảng. Indexer đẩy các sự kiện giao dịch on-chain đã chuẩn hoá; `insight` consume những cái đó và biến thành PnL/ROI theo từng user, ranking token trending, và cảnh báo giá. Nó không nằm trên đường thực thi giao dịch — nó là tầng dữ liệu người dùng xem lại sau đó."
2. "Nó chỉ biết Sui, và hình dạng của nó là đồng bộ, đặc thù Sui. Mở rộng nghĩa là luồn các giả định về chuỗi vào khắp nơi. Thêm chuỗi kiểu đó sẽ nhân bug lên, nên quyết định thành thật là prototype là ngõ cụt so với hướng đi của sản phẩm."
3. "Cả nền tảng là Go, nên giữ Node nghĩa là một hòn đảo lạc lõng phải vận hành. Go cũng cho em concurrency rẻ cho các consumer throughput cao và footprint deploy/K8s đơn giản hơn. Không phải 'Node dở' — mà là tính nhất quán cộng với sự phù hợp hơn cho một consumer nặng về throughput."
4. "Nguồn sự kiện là Kafka. Indexer publish các sự kiện balance-change/swap đã chuẩn hoá về một schema chung. `insight` phản ứng với các sự kiện đó thay vì pull hay tính đồng bộ — nên ingest, compute và serve tách rời, mỗi phần hỏng và hồi phục độc lập."
5. "Ba đường ranh giới: một tầng ingest đọc Kafka và chuẩn hoá, một tầng compute sở hữu logic PnL/ranking/alert và state, và một tầng serve (API) đọc kết quả đã materialize. Compute không bao giờ đụng wire format; serve không bao giờ đụng Kafka."
6. "Mọi thứ đặc thù chuỗi nằm sau normalization boundary — đọc một swap, decimals, định dạng địa chỉ, quirk của event. Phía trên ranh giới đó, một sự kiện chỉ là (ví, token, số lượng, chiều, timestamp). Code lõi PnL và ranking viết một lần dựa trên hình dạng đã chuẩn hoá đó và không bao giờ rẽ nhánh theo chuỗi."
7. "Chuỗi EVM dựa trên log/receipt với quy ước 18 decimals và địa chỉ hex; Sui có object model riêng và decimals riêng; Solana dựa trên account với cấu trúc tx hoàn toàn khác. Khác biệt đều ở chỗ 'làm sao đọc một giao dịch và số lượng của nó' — đúng thứ em cô lập trong các adapter."
8. "Em chuẩn hoá mọi thứ về biểu diễn số nguyên fixed-precision với decimals rõ ràng cho từng token, không bao giờ dùng float. Mỗi adapter chịu trách nhiệm chuyển số lượng thô của chuỗi mình sang dạng canonical đó, nên lõi không bao giờ phải đoán decimals — nó được cho biết ở ranh giới."
9. "Sự kiện lỗi bị validate ở ranh giới ingest và bị reject/dead-letter thay vì làm hỏng cả batch. Sai thứ tự thì được xử lý vì PnL là một phép tính lại từ balance-change gom theo user, và em commit offset thủ công — nên một sự kiện xấu không làm hỏng state của user, nó chỉ bị bỏ qua và đánh dấu."
10. "Nó chạy song song — consumer Go đọc cùng topic với consumer TS nhưng ghi vào bảng riêng trước, để em diff output trước khi chuyển reads sang. Khả năng replay của Kafka làm việc này an toàn: em có thể reset offset và xử lý lại nếu số mới bị lệch."
11. "Với phần lõi em thiên về viết lại vì hình dạng của prototype mới là vấn đề, không phải vài hàm. Nhưng rollout theo kiểu strangler: chạy song song, diff, rồi cutover từng chuỗi. Tức là viết lại phần ruột, migrate traffic tăng dần."
12. "Event-driven đánh đổi sự đơn giản và tính nhất quán tức thời — kết quả là eventually consistent và debug là 'lần theo event' thay vì stack trace. Em đổi cái đó lấy sự tách rời, khả năng replay, và chịu được back-pressure, những thứ quan trọng hơn với một data pipeline so với độ tươi sub-giây."
13. "Lưới an toàn là diff chạy-song-song cộng với alert về consumer-lag và error-rate. Nếu một lần launch chuỗi làm regress PnL, cái diff so với pipeline cũ hoặc một cú tăng vọt event bị dead-letter sẽ lộ ra. Vì Kafka giữ lại event, remediation thường là 'sửa adapter, replay các offset bị ảnh hưởng.'"
14. "Em diff output đã materialize — cùng topic đầu vào, hai bảng đầu ra, so sánh PnL/ROI theo từng ví trong một khoảng thời gian. Lệch nhỏ do thứ tự thì em giải thích được; bất cứ lệch mang tính cấu trúc nào nghĩa là có bug chuẩn hoá cần sửa trước cutover."
15. "Thêm chuỗi chủ yếu là thêm partition và thêm consumer instance — nó scale ngang vì công việc được phân vùng. Chi phí thật không phải CPU, mà là state: bộ nhớ gom theo user và khối lượng ghi Postgres. Đó là chỗ em nhìn đầu tiên, không phải số lượng consumer."
16. "Consumer scale ra được, nên điểm áp lực đầu tiên là phía sau — throughput upsert Postgres và cái map gom-theo-user trong bộ nhớ bị 'nóng' với các ví cực kỳ active. Em sẽ shard/tune đường ghi và theo dõi consumer lag trước khi bản thân phần compute trở thành giới hạn."
17. "Một service xử lý tất cả các chuỗi qua adapter là đúng cho một team nhỏ — một thứ để vận hành, chung logic lõi. Microservice cho mỗi chuỗi sẽ nhân bội bề mặt deploy/ops cho một team một-tới-vài người. Nếu khối lượng của một chuỗi lấn át phần còn lại, em sẽ tách *chuỗi đó* ra, chứ không shard mặc định."
18. "Em có cân nhắc. Với quy mô và team của bọn em, consumer Go tự viết đơn giản để vận hành và debug hơn là dựng Flink, và logic của bọn em (recompute gom theo user + upsert) không phải một job windowed-streaming tự nhiên. Nếu aggregation phức tạp hơn nhiều, Kafka Streams/Flink mới đáng cái trọng lượng vận hành."
19. "Kafka cho bọn em parallelism theo partition, retention/replay, và back-pressure — em dùng cả ba một cách chủ động (replay lúc cutover, lag làm tín hiệu sức khoẻ). Poll chuỗi trực tiếp thì bó compute vào tình trạng sẵn sàng của RPC; một queue thường không cho em replay hay partition có thứ tự."
20. "Em sẽ chuẩn hoá schema event từ ngày đầu và version nó — em có tiến hoá nó hơi tuỳ hứng. Và em sẽ tách state aggregation theo user thành một thứ tường minh hơn sớm hơn, vì đó là phần sau này cần chăm sóc nhiều nhất khi tải cao."

### 7. Whiteboard Version
Vẽ **từ trái sang phải**. Bắt đầu bằng **nguồn sự kiện**: chains → indexers → Kafka (vẽ topic với vài
partition). *Đây là thứ vẽ đầu tiên — nó định khung cho mọi thứ.* Rồi vẽ các consumer `insight` thành một
hộp, và **bên trong** vẽ ba tầng xếp chồng: `ingest/normalize`, `compute (PnL/rank/alert)`, `serve`. Sau đó
vẽ mũi tên ra **Postgres** (PnL đã materialize) và **Redis** (rankings/pub-sub). Vẽ **normalization boundary
thành một đường đứt nét dọc** ngay sau ingest và nói "mọi thứ bên trái là đặc thù chuỗi, mọi thứ bên phải là
chain-agnostic" — người phỏng vấn gần như luôn ngắt lời ở đây để dò về sự trừu tượng, đúng chỗ bạn mạnh. Họ
cũng ngắt ở phần Kafka partition để hỏi về thứ tự (chuyển sang R2).

### 8. Lỗi Thường Gặp
- **"Em viết lại vì TypeScript dở."** Nghe non/giáo điều. Tốt hơn: "Hình dạng của prototype không hợp đa chuỗi; Go hợp với stack và nhu cầu throughput của bọn em." Đánh giá *sự phù hợp*, không phải ngôn ngữ.
- **"Em tự làm hết mọi thứ."** Nghe như red flag về teamwork. Tốt hơn: "Em own migration ban đầu một mình, rồi nó thành nền team xây tiếp" — vừa solo vừa hợp tác.
- **Lao vào chi tiết object-model của Sui khi chưa ai hỏi.** Bạn mất mạch. Giữ câu chuyện trừu tượng trước; chỉ đào sâu khi được hỏi.

### 9. Red Flags
- "5+ chuỗi" — sẵn sàng gọi tên chúng và nói cái gì thực sự khác nhau; nếu vài cái là adapter mỏng, nói thật ("các chuỗi EVM dùng chung phần lớn adapter"). Cách nói dễ bảo vệ: *"năm-cộng chuỗi, dù ba chuỗi EVM dùng chung khá nhiều adapter."*
- "gần như một mình" — làm mềm thành *"em đẩy phần lớn migration ban đầu trước khi team lớn lên"* để không nghe như bạn nhận hết công cả nền tảng.
- "re-architected" — phải phân biệt được viết lại (phần ruột) với migration (rollout); nói quá thành "re-architected cả nền tảng" rủi ro hơn "re-architected service `insight`."

### 10. Kịch Bản Chốt
"Cái em hay nhắc đầu tiên là service `insight` ở RaidenX — nó là backend dữ liệu giao dịch đứng sau PnL,
rankings, và alerts. Lúc em nhận, nó là một prototype TypeScript một-chuỗi chỉ xử lý Sui, mà bọn em thì biết
sẽ đi đa chuỗi, nên nó là ngõ cụt. Em viết lại bằng Go theo hướng event-driven — nó consume các sự kiện đã
chuẩn hoá từ Kafka thay vì xử lý đồng bộ — và quyết định mấu chốt là đẩy toàn bộ logic đặc thù chuỗi ra sau
một normalization boundary. Phía trên đường đó, một sự kiện chỉ là ví, token, số lượng, chiều — nên logic PnL
và ranking viết một lần và không bao giờ rẽ nhánh theo chuỗi. Đó là thứ giúp bọn em mở rộng từ Sui sang BSC,
Base, Solana, Monad mà lõi không rối thêm. Em đẩy phần lớn migration đó một mình giai đoạn đầu, và cuối cùng
nó thành thứ team xây lên trên khi bọn em lớn tới tám mười người. Em có thể đi sâu vào cách xử lý thứ tự qua
các partition hay chuyện cutover, tuỳ cái nào hữu ích hơn ạ."

---

## Bullet R2 — Own pipeline PnL/ROI (batched Kafka consumers, thứ tự per-user, at-least-once + dedup)

> *"Owned the PnL/ROI pipeline: batched Kafka consumers aggregate balance-change events per wallet/token
> across ~20–25 partitions, recompute PnL/ROI, and upsert to PostgreSQL with in-batch dedup and duplicate-key
> retry (at-least-once, manual offset commit, in-consumer per-user ordering)."*

### 1. Ý đồ (Intent)
Đây là bullet **tính đúng đắn của hệ phân tán** — tín hiệu kỹ thuật sâu nhất trong CV. Toán tiền + ngữ nghĩa
Kafka + thứ tự mà không có đảm bảo từ partition đúng là loại chuyện phân biệt "viết một consumer" với "hiểu vì
sao consumer khó." Ấn tượng: *người này suy luận cẩn thận về delivery guarantee, thứ tự, và idempotency, và
biết tiền thì không được sai.*

### 2. Elevator Pitch (20–30 giây)
"Phần em own sâu nhất là pipeline PnL và ROI. Mỗi giao dịch tạo ra các sự kiện balance-change, bọn em consume
chúng từ Kafka, gom theo ví và token, tính lại PnL, rồi upsert vào Postgres. Chỗ hóc búa là nó là tiền và Kafka
là at-least-once, nên em phải làm cả pipeline idempotent — dedup trong batch, retry duplicate-key khi upsert,
commit offset thủ công — và em phải giữ thứ tự cập nhật của từng user dù các partition không được chia theo user."

### 3. Bản Sâu (1–2 phút)
"Ràng buộc miền (domain) chi phối mọi thứ ở đây: nó là tiền. PnL và ROI là những con số người dùng ra quyết định
dựa trên đó, nên 'gần đúng' là không chấp nhận được — một sự kiện bị đếm hai lần nghĩa là số dư sai.

Pipeline: indexer publish sự kiện balance-change lên Kafka qua khoảng 20–25 partition. Consumer của em đọc theo
batch, gom trong bộ nhớ theo ví và token, tính lại PnL/ROI cho từng user bị ảnh hưởng, rồi upsert kết quả vào
Postgres. Em chọn batched thay vì per-message một cách có chủ đích — đó là một câu chuyện riêng về sự cố lag —
nhưng phần thiết kế tính đúng đắn mới là phần thú vị.

Kafka cho bạn at-least-once, nghĩa là gửi lại là bình thường, không phải ngoại lệ. Nên em thiết kế cho điều đó.
Thứ nhất, dedup trong batch: trong một batch em gộp các sự kiện trùng để cùng một sự kiện không thể được áp hai
lần trong một lượt. Thứ hai, phép ghi Postgres là upsert với khoá duy nhất, và em xử lý conflict duplicate-key
bằng retry/merge thay vì để nó lỗi. Thứ ba, em commit offset thủ công — chỉ sau khi batch đã được lưu thành công
— nên crash giữa batch sẽ xử lý lại chứ không âm thầm bỏ qua. At-least-once cộng với ghi idempotent cho bạn ngữ
nghĩa effectively-once mà không cần exactly-once.

Vấn đề thứ tự mới là chỗ tinh tế. Các partition do DevOps cấp và không chia theo user — nên sự kiện của một user
có thể rơi vào các partition khác nhau và bị xử lý bởi các consumer khác nhau. Với PnL điều đó nguy hiểm vì thứ
tự quan trọng. Cách sửa của em là biến thứ tự thành một thuộc tính của compute, không phải của transport: trong
một batch em gom theo user và áp các sự kiện của mỗi user theo thứ tự timestamp, và vì PnL là phép tính lại từ
balance-change chứ không phải cộng dồn mù quáng, kết quả đã materialize hội tụ về đúng. Nên em không đấu với Kafka
để bắt nó cho thứ tự per-user toàn cục — em làm cho phép tính chịu được đúng cái thứ tự mà Kafka thực sự cho.

Kết quả: PnL đúng dưới việc gửi lại và trải qua nhiều partition, không đếm trùng. Bài học là săn exactly-once ở
tầng transport thường là nước đi sai — ghi idempotent cộng một mô hình thân thiện với recompute thì đơn giản và
bền hơn nhiều."

### 4. Interview Hooks
- "at-least-once … dedup trong batch … retry duplicate-key" → *Bạn dedup chính xác thế nào? Idempotency key là gì?*
- "thứ tự per-user trong consumer … partition không chia theo user" → *Sắp thứ tự thế nào khi không có thứ tự partition? Race giữa các consumer?*
- "tính lại PnL/ROI" → *Tính lại từ gì? Toàn bộ lịch sử hay incremental?*
- "commit offset thủ công" → *Commit lúc nào? Crash giữa batch thì sao?*
- "upsert vào PostgreSQL" → *Contention? Ranh giới transaction? Throughput?*

### 5. Câu Hỏi Đào Sâu
1. (L1) PnL/ROI ở đây là gì và vì sao tính đúng đắn lại quan trọng đến vậy?
2. (L1) Một sự kiện balance-change là gì và đến từ đâu?
3. (L2) Dẫn tôi đi qua một batch từ đầu tới cuối.
4. (L2) Vì sao consumer batched thay vì per-message?
5. (L2) Idempotency key của bạn là gì, và vì sao chọn cái đó?
6. (L3) Dedup trong batch thực sự hoạt động thế nào ở mức code?
7. (L3) Bạn xử lý conflict duplicate-key khi upsert thế nào — merge hay ignore?
8. (L3) Bạn commit offset chính xác lúc nào, và vì sao thủ công?
9. (L3) Tính lại PnL từ state nào — toàn bộ lịch sử, snapshot + delta, hay running total?
10. (L4) Consumer crash sau khi ghi Postgres nhưng trước khi commit offset — chuyện gì xảy ra?
11. (L4) Consumer crash sau khi commit nhưng trước khi ghi — chuyện gì xảy ra?
12. (L4) Hai consumer xử lý sự kiện của cùng một user từ các partition khác nhau đồng thời — race?
13. (L4) Một sự kiện đến trễ với timestamp cũ hơn sau khi bạn đã tính xong — giờ sao?
14. (L5) Vì sao không dùng exactly-once (Kafka transactions) thay vì upsert idempotent?
15. (L5) Trade-off batch size — quá lớn hay quá nhỏ thì sao?
16. (L6) Upsert Postgres thành nút thắt khi tải cao — chẩn đoán và sửa thế nào?
17. (L6) Bạn phát hiện PnL sai cho một nhóm user tuần trước — hồi phục thế nào?
18. (L7) Traffic tăng 10x — pipeline này hỏng ở đâu trước và bạn làm gì?
19. (L7) Một ví cá voi tạo ra 100x sự kiện so với người khác — bài toán hot-key — xử lý thế nào?
20. (L8) Bạn có dùng Kafka Streams / một stateful stream processor với changelog topic không? Trade-off?

### 6. Câu Trả Lời Chuẩn
1. "PnL là lãi/lỗ đã thực hiện + chưa thực hiện theo ví và token; ROI là so với cost basis. Người dùng giao dịch dựa trên các con số này, nên một sự kiện đếm trùng nghĩa là hiển thị cho ai đó một số dư sai — nó là phần nhạy cảm về tính đúng đắn nhất của nền tảng."
2. "Bất cứ khi nào holdings của một ví thay đổi do giao dịch, indexer phát một sự kiện balance-change — ví, token, số lượng, chiều, timestamp — lên Kafka. Pipeline của em là consumer của những cái đó; em không đọc chuỗi trực tiếp."
3. "Poll một batch từ các partition được gán, validate và dedup trong batch, gom sự kiện theo ví/token trong một map bộ nhớ, áp từng nhóm theo thứ tự timestamp để tính lại PnL/ROI, upsert kết quả vào Postgres trong một transaction, và chỉ sau đó mới commit offset."
4. "Per-message nghĩa là một round-trip Postgres và một commit offset cho mỗi sự kiện, mà ở khối lượng của bọn em thì consumer không theo kịp — đó chính là thứ gây ra sự cố lag. Batching phân bổ đều chi phí ghi DB và cho em dedup và gom trước khi đụng DB."
5. "Khoá tự nhiên là (ví, token) cho hàng đã materialize, và per-event là id duy nhất của sự kiện (kiểu tx hash + log index). Event id cho em dedup; khoá (ví, token) cho em mục tiêu upsert idempotent."
6. "Em xây một map key theo event id khi ingest batch, nên một sự kiện gửi lại có cùng id chỉ ghi đè/gộp — batch tới được compute có nhiều nhất một bản của mỗi sự kiện. Rồi em gom tập đã dedup theo user để sắp thứ tự."
7. "Là merge, không phải ignore mù. Nhánh ON CONFLICT của upsert tính lại/gộp hàng thay vì bỏ phép ghi, vì batch mới có thể mang state mới hơn. Ignore sẽ có nguy cơ mất một cập nhật hợp lệ dùng chung khoá."
8. "Sau khi transaction Postgres commit thành công. Commit thủ công là cả điểm mấu chốt — auto-commit có thể đẩy offset trước khi phép ghi của em xuống DB, và như vậy sẽ âm thầm mất dữ liệu khi crash. Commit-sau-khi-lưu cho em at-least-once mà không mất dữ liệu."
9. "Là phép tính lại từ balance state chứ không phải cộng dồn mù — điều đó có chủ đích, vì recompute vốn idempotent. Áp cùng một tập balance-change hai lần hội tụ về cùng kết quả, còn increment cộng dồn thì sẽ nhân đôi."
10. "Đó là trường hợp an toàn: offset chưa được đẩy, nên khi khởi động lại em xử lý lại batch đó. Vì phép ghi là upsert idempotent và sự kiện đã dedup, xử lý lại cho ra cùng kết quả — không đếm trùng. Đó chính là lý do commit đứng cuối."
11. "Điều đó không thể xảy ra với thứ tự của em — em không bao giờ commit trước khi ghi. Nếu bằng cách nào đó nó xảy ra, em mất một batch, nên bất biến (invariant) là ghi-rồi-mới-commit một cách nghiêm ngặt. Thứ tự của hai thao tác này là toàn bộ đảm bảo an toàn."
12. "Chúng không thể làm hỏng nhau vì phép ghi là upsert idempotent theo khoá (ví, token) và compute là recompute, không phải increment. Tệ nhất là một phép ghi thừa; merge ON CONFLICT giải quyết. Em làm tính đúng đắn độc lập với concurrency thay vì dựa vào lock."
13. "Vì PnL là recompute từ balance-change chứ không phải một running sum có thứ tự, một sự kiện trễ chỉ kích hoạt một recompute bao gồm nó. Trong một batch em sort theo timestamp; xuyên batch thì mô hình recompute-từ-state nghĩa là thứ tự đến không làm hỏng con số cuối."
14. "Kafka transactions/exactly-once thêm overhead thật và ràng bạn vào các config producer/consumer cụ thể, mà vẫn không giúp gì cho bài toán thứ tự xuyên partition. Upsert idempotent + commit thủ công cho em effectively-once ở sink với ít phức tạp hơn nhiều — DB là source of truth, nên hãy làm phép ghi an toàn."
15. "Quá nhỏ thì quay lại overhead per-message — round-trip DB lấn át và lag tăng. Quá lớn thì tăng áp lực bộ nhớ, latency mỗi batch, và chi phí xử lý lại khi hỏng. Em tune để giữ lag phẳng mà batch không lớn tới mức retry đắt."
16. "Đầu tiên xác nhận đó là sink qua việc consumer lag tăng trong khi CPU rảnh — dấu hiệu kinh điển 'đang chờ DB'. Rồi giảm write amplification: batch hiệu quả lớn hơn, bulk upsert, ít round-trip hơn, và tune index/lock trên khoá conflict. Nếu một bảng bị nóng, partition hoặc shard đường ghi."
17. "Vì Kafka giữ lại sự kiện và phép ghi của em idempotent, hồi phục là replay: reset consumer về trước khoảng bị ảnh hưởng và xử lý lại. Mô hình recompute nghĩa là replay không thể đếm trùng — chính cái replay-safety đó là lý do chính em thiết kế theo cách này."
18. "Consumer scale ngang theo partition, nên thứ hỏng đầu tiên là throughput upsert Postgres và cái map gom-theo-user trong bộ nhớ cho các ví nóng. Em sẽ scale đường ghi (bulk upsert, shard bảng nóng) trước khi thêm consumer, vì consumer không phải nút thắt."
19. "Đó là bài toán hot-key. Batching đã giúp vì sự kiện của một cá voi gộp thành ít recompute hơn mỗi batch. Nếu một ví vẫn lấn át một partition, lựa chọn là chia nhỏ công việc của nó hoặc chấp nhận latency hơi cao hơn cho key đó — nhưng tính đúng đắn vẫn giữ vì nó vẫn là recompute idempotent theo khoá."
20. "Kafka Streams với state store có changelog là một lựa chọn hợp lệ — bạn có local state chịu lỗi cho aggregation theo user. Em chọn consumer Go tự viết + Postgres vì Postgres đã là source of truth và em muốn kết quả materialize query trực tiếp được; Streams sẽ thêm một hệ state thứ hai phải vận hành. Nếu logic aggregation lớn lên, em sẽ xem lại."

### 7. Whiteboard Version
Vẽ **Kafka với ~20–25 partition** trước (một hình chữ nhật chia thành các lane partition) và nhấn "không chia
theo user" — đó là bối cảnh cho bài toán thứ tự. Rồi vẽ một **hộp consumer** kéo một **batch**, và bên trong vẽ
pipeline thành một dải: `dedup → group-by-user → sort-by-ts → recompute → upsert`. Vẽ **bảng Postgres** với khoá
duy nhất và ghi nhãn mũi tên "idempotent upsert (ON CONFLICT merge)". Cuối cùng vẽ **commit offset** thành một
mũi tên riêng quay về Kafka *sau* DB, và khoanh tròn: "commit cuối = at-least-once không mất mát." Người phỏng
vấn gần như luôn ngắt ở (a) "partition không chia theo user — vậy sắp thứ tự thế nào?" và (b) "at-least-once —
làm sao tránh đếm trùng?" Cả hai là câu trả lời mạnh nhất của bạn, nên hãy mời họ hỏi.

### 8. Lỗi Thường Gặp
- **"Kafka cho exactly-once nên em không lo trùng."** Mất uy tín ngay — mặc định của Kafka là at-least-once. Tốt hơn: "Em coi gửi lại là bình thường và làm phép ghi idempotent."
- **"Em dùng lock để chống race."** Ngụ ý bạn không tìm ra mô hình đơn giản hơn. Tốt hơn: "Em làm phép ghi idempotent và compute là recompute, nên concurrency không thể làm hỏng state — không cần lock."
- **"Em giữ một running total."** Mở đường cho các câu hỏi đếm trùng mà bạn không thắng nổi. Tốt hơn: "Là recompute từ balance state, vốn idempotent."

### 9. Red Flags
- "~20–25 partition" — biết vì sao con số đó (throughput/parallelism, do DevOps cấp) và rằng bạn không chọn nó; sở hữu ranh giới đó một cách thành thật.
- "recompute PnL" — nói chính xác *tính lại từ gì*; nếu là snapshot + delta gần đây chứ không phải toàn bộ lịch sử, nói ra, kẻo "recompute toàn bộ lịch sử mỗi event" nghe đắt và gây nghi ngờ.
- "at-least-once, effectively-once" — đừng nói "exactly-once"; hãy nói "ghi idempotent cho effectively-once ở sink", vừa chính xác vừa nghe mạnh hơn với một senior.

### 10. Kịch Bản Chốt
"Phần em own sâu nhất là pipeline PnL/ROI, và điều thú vị là nó là tiền, nên tính đúng đắn chi phối mọi quyết
định. Các sự kiện balance-change đến từ Kafka qua khoảng hai mươi partition; consumer của em đọc theo batch,
dedup trong batch, gom theo ví và token, và tính lại PnL — em tính lại từ balance state chứ không giữ running
total, cụ thể vì recompute là idempotent. Phép ghi vào Postgres là upsert với conflict-merge, và em commit offset
Kafka thủ công chỉ sau khi phép ghi đó xuống. Nên dù Kafka là at-least-once và gửi lại là bình thường, áp cùng
các sự kiện hai lần vẫn hội tụ về cùng con số — effectively-once ở sink mà không cần Kafka transactions. Chỗ tinh
tế là thứ tự: các partition không chia theo user, nên em biến thứ tự thành thuộc tính của compute — gom theo user,
áp theo thứ tự timestamp — thay vì dựa vào transport. Em có thể đi sâu hơn vào các case hồi phục sau crash hay bài
toán ví nóng nếu hữu ích ạ."

---

## Bullet R3 — Chẩn đoán & xử lý sự cố Kafka consumer-lag ~4 triệu message (per-message → batched)

> *"Diagnosed and remediated a Kafka consumer-lag incident (~4M-message backlog in hours) by redesigning
> ingestion from per-message to batched processing — cutting steady-state lag from millions to the low
> hundreds and raising sustained throughput."*

### 1. Ý đồ (Intent)
Đây là bullet **sự cố production / debug dưới áp lực**. Nó cho thấy bạn vận hành được hệ thống đang chạy, tìm
root cause nhanh, và ship một fix giữ vững. Con số (~4M → vài trăm) làm nó cụ thể. Ấn tượng: *người này là người
bạn muốn có mặt lúc on-call khi pipeline bốc cháy — bình tĩnh, có phương pháp, và fix mang tính kiến trúc, không
phải vá tạm.*

### 2. Elevator Pitch (20–30 giây)
"Bọn em có một sự cố consumer lag bùng nổ — khoảng bốn triệu message tồn đọng trong vài tiếng. Em đào vào và root
cause là bọn em xử lý message Kafka từng cái một, nên mỗi message là một round-trip DB và một commit offset riêng,
và consumer không theo kịp tốc độ produce. Em thiết kế lại ingestion để xử lý theo batch, và cái đó kéo steady-state
lag từ hàng triệu xuống còn vài trăm và nâng sustained throughput lên nhiều."

### 3. Bản Sâu (1–2 phút)
"Một hôm bọn em bắt đầu thấy consumer lag leo nhanh — trong vài tiếng nó tụt tầm bốn triệu message, và không hồi
phục, mà còn tăng. Đó là vấn đề thật vì đây là pipeline dữ liệu giao dịch, nên lag tăng nghĩa là PnL và rankings
của người dùng ngày càng cũ.

Việc đầu tiên em làm là đặc tả nó: là do produce spike, downstream chậm, hay bản thân consumer? Lag leo trong khi
CPU consumer không đầy, đó là dấu hiệu kinh điển của 'đang chờ cái gì đó' chứ không phải 'compute-bound'. Đào vào,
consumer đang xử lý per-message — một message vào, làm việc, ghi Postgres, commit offset, lặp lại. Nên mỗi message
trả toàn bộ chi phí round-trip DB cộng một commit. Ở khối lượng bình thường thì ổn; dưới tốc độ produce cao hơn,
overhead per-message khiến tốc độ consume tối đa của bọn em chỉ vừa dưới tốc độ produce, và lag tăng vô hạn một
khi điều đó xảy ra.

Ràng buộc là em không thể chỉ 'thêm consumer' — bọn em đã gần số partition rồi, mà quăng thêm consumer vào một
thiết kế per-message thì vẫn trả overhead per-message. Fix thật là tấn công cái overhead. Nên em thiết kế lại
ingestion để batch: poll một batch message, dedup và gom, rồi làm việc bulk với Postgres — một transaction và một
commit offset cho mỗi batch thay vì mỗi message. Cái đó xẹp đi chi phí chủ đạo. Em cũng phải giữ tính đúng đắn khi
làm việc đó, và đó là chỗ dedup trong batch và upsert idempotent ra đời.

Kết quả: steady-state lag tụt từ hàng triệu xuống vài trăm, và sustained throughput tăng đủ để bọn em có headroom
thật thay vì chạy ở mép. Bài học là chẩn đoán trước khi phản ứng — nước đi hấp dẫn là scale ra, nhưng nút thắt là
overhead per-message, và không lượng scale ngang nào sửa được một chi phí O(1)-mỗi-message vốn đã quá cao."

### 4. Interview Hooks
- "chẩn đoán … ~4M tồn đọng trong vài tiếng" → *Chẩn đoán thế nào? Metric/tool gì?*
- "per-message sang batched" → *Vì sao per-message hỏng? Batch size? Tính đúng đắn khi batch?*
- "kéo lag từ hàng triệu xuống vài trăm" → *Verify thế nào? Nó có giữ dưới tải không?*
- "nâng sustained throughput" → *Nút thắt mới sau đó là gì?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Consumer lag là gì và vì sao 4M lại tệ ở đây cụ thể?
2. (L1) Bạn để ý ra sự cố lần đầu thế nào?
3. (L2) Các nguyên nhân khả dĩ của lag tăng là gì và bạn thu hẹp thế nào?
4. (L2) Vì sao xử lý per-message giới hạn throughput của bạn?
5. (L3) Thiết kế batched thay đổi cụ thể những gì (DB, commit, dedup)?
6. (L3) Bạn chọn batch size thế nào?
7. (L3) Bạn giữ PnL đúng thế nào khi batch (dedup, thứ tự)?
8. (L4) Trong sự cố, dữ liệu bị mất hay chỉ bị trễ? Làm sao bạn biết?
9. (L4) Nếu batch hỏng giữa chừng — ghi một phần thì sao?
10. (L4) Bạn drain 4M backlog an toàn thế nào mà không gây sự cố thứ hai?
11. (L5) Batching thêm latency mỗi message — trade-off đó có chấp nhận được không? Vì sao?
12. (L5) Vì sao không chỉ thêm partition/consumer?
13. (L6) Bạn ngăn tái diễn thế nào — alert, autoscaling, load test?
14. (L6) Kế hoạch rollback nếu consumer batched cư xử sai trên prod là gì?
15. (L6) Dẫn tôi qua runbook on-call hiện tại cho alert này.
16. (L7) Ở 10x tốc độ produce, batching có còn giữ không? Cái gì hỏng tiếp?
17. (L7) Sau batching, nút thắt mới là gì?
18. (L8) Buffer phía consumer + ghi DB async có được không?
19. (L8) Bạn có thể dùng đường bulk-load (COPY) thay vì upsert dưới áp lực sự cố không?
20. (L8) Nếu bạn có Kafka Streams, sự cố này có xảy ra không?

### 6. Câu Trả Lời Chuẩn
1. "Lag là consumer đang cách offset mới nhất bao xa — 4M nghĩa là PnL của người dùng cũ hàng triệu event và ngày càng tệ. Với một nền tảng dữ liệu giao dịch, đó không chỉ là chậm, mà là dữ liệu sai đang hiển thị trên màn hình."
2. "Monitoring/alert lag trên consumer group — metric lag leo đơn điệu thay vì dao động quanh 0, đó là dấu hiệu tốc độ consume < tốc độ produce."
3. "Ba nhóm: produce spike, consumer chậm, hoặc downstream chậm. Em loại produce spike thuần vì nó không hồi phục, và em thấy CPU consumer không bão hoà trong khi lag tăng — cái đó chỉ vào overhead per-message I/O-bound, tức đang chờ Postgres và commit, không phải compute."
4. "Mỗi message trả một chi phí cố định — một round-trip DB và một commit offset. Chi phí cố định per-message đó đặt trần message/giây bất kể kích thước message. Một khi produce vượt trần đó, lag tăng vô hạn; bạn không thể scale-ra vượt một chi phí per-item vốn quá cao về mặt cấu trúc."
5. "Poll N message, dedup và gom trong bộ nhớ, ghi bulk/transactional vào Postgres, và commit một offset cho mỗi batch. Nên round-trip DB và commit đi từ per-message xuống per-batch — chi phí chủ đạo tụt gần bằng batch factor."
6. "Bằng thực nghiệm — đủ lớn để phân bổ đều chi phí DB và làm phẳng lag, đủ nhỏ để giới hạn bộ nhớ và giữ latency per-batch và chi phí retry hợp lý. Em tune bằng cách theo dõi lag và latency batch thay vì chọn một con số ma thuật."
7. "Dedup trong batch theo event id để gửi lại không đếm hai lần, gom theo user và áp theo thứ tự timestamp, và upsert idempotent theo khoá (ví, token). Batching thực ra làm tính đúng đắn dễ hơn vì em có thể dedup cả một batch trước khi đụng DB."
8. "Trễ chứ không mất. Kafka giữ lại mọi thứ và offset chỉ tiến sau khi ghi thành công, nên 4M vẫn nằm trên topic — fix giúp consumer đuổi kịp bằng cách drain chúng, và idempotency nghĩa là xử lý lại an toàn."
9. "Phép ghi batch là transactional và offset chỉ commit sau khi nó thành công, nên hỏng giữa batch sẽ rollback và cả batch xử lý lại. Upsert idempotent + dedup làm việc xử lý lại đó an toàn — không đếm trùng một phần."
10. "Deploy consumer batched và để nó consume nhanh hơn produce — lag drain đơn điệu. Em theo dõi để nó không gây lỗi khi drain; vì phép ghi idempotent nên em có thể để nó chạy hết công suất mà không sợ hỏng dữ liệu."
11. "Có — bọn em đổi một chút latency per-message để giữ trạng thái đuổi kịp. Với một pipeline dữ liệu/analytics, tươi hơn vài trăm ms mỗi message là vô nghĩa nếu bạn đang tụt hàng triệu. Latency có giới hạn, dự đoán được thắng lag vô hạn."
12. "Thêm partition/consumer không loại bỏ chi phí DB per-message — mỗi consumer vẫn trả nó, và bọn em đã gần số partition. Scale ra một thiết kế kém hiệu quả về cấu trúc chỉ trải rộng sự kém hiệu quả; em sửa hằng số nhân thay vào đó."
13. "Alert lag với ngưỡng và xu hướng thật, cộng với thiết kế batched cho headroom để spike bình thường không tới gần trần. Load-test consumer với tốc độ produce cao hơn để bọn em biết trần mới trước khi prod tự tìm ra cho bọn em."
14. "Consumer batched có thể rollback về image trước, và vì offset chỉ tiến khi thành công và sự kiện được giữ lại, rollback không mất dữ liệu — tệ nhất là xử lý lại. Idempotency là thứ làm rollback an toàn."
15. "Alert kích hoạt theo xu hướng lag → kiểm tra là produce spike (hồi phục) hay trần consume (không hồi phục) → kiểm tra CPU consumer vs I/O wait → nếu I/O-bound, xem sức khoẻ DB/batch; nếu produce spike, xác nhận nó drain. Runbook mã hoá đúng cái chẩn đoán em làm lúc đó."
16. "Batching mua một bội số lớn headroom, nên 10x hấp thụ được tới điểm throughput upsert Postgres hoặc bộ nhớ gom-theo-user bão hoà — đó thành giới hạn tiếp theo, và em sẽ scale đường ghi (bulk upsert/sharding) ở đó."
17. "Là sink — throughput upsert Postgres và, với các ví rất nóng, cái map gom trong bộ nhớ. Nút thắt dời từ 'overhead consumer' sang 'ghi bền được nhanh cỡ nào', đó là một chỗ tốt hơn nhiều để đứng."
18. "Buffer async giúp nhưng thêm failure mode riêng — bạn có thể mất buffer khi crash và giờ bạn cần durability cho nó. Batching với commit-sau-khi-lưu cho cùng sự phân bổ đều với chính Kafka làm buffer bền, đơn giản và an toàn hơn."
19. "COPY nhanh hơn cho insert thuần, nhưng bọn em cần ngữ nghĩa upsert (conflict-merge) cho tính đúng đắn, nên bulk upsert hợp hơn. Dưới áp lực sự cố em ưu tiên một fix đúng-theo-thiết-kế hơn là đường ghi nhanh tuyệt đối."
20. "Có thể ít khả năng hơn — Streams batch và quản state cho bạn — nhưng cùng root cause (một sink quá chậm mỗi item) vẫn có thể cắn bạn. Framework sẽ không loại bỏ nhu cầu nghĩ về write amortization; nó chỉ giấu đi cho tới khi không giấu được nữa."

### 7. Whiteboard Version
Vẽ một bức tranh **tốc độ produce vs consume** trước — hai mũi tên vào và ra khỏi một Kafka topic, với mũi tên
"ra" mảnh hơn "vào", và một thanh backlog đang lớn. Cái đó minh hoạ trực quan "lag tăng khi consume < produce."
Rồi vẽ **trước** (per-message: message → DB → commit, lặp lại, khoanh tròn "round-trip DB mỗi message" như chi
phí) và **sau** (batch → dedup/gom → ghi bulk → một commit). Người phỏng vấn ngắt ở "làm sao biết là consumer chứ
không phải bản thân DB" — trả lời bằng dấu hiệu CPU-rảnh-trong-khi-lag-tăng. Họ cũng dò "sao không chỉ thêm
consumer" — đánh vào điểm trần-chi-phí-per-message.

### 8. Lỗi Thường Gặp
- **"Em thêm consumer/partition và nó tự khỏi."** Ngụ ý bạn chữa triệu chứng. Tốt hơn: "Scale ra không loại bỏ chi phí per-message; em sửa hằng số nhân bằng batching."
- **"Em restart service và lag giảm."** Nghe như may mắn, không phải chẩn đoán. Tốt hơn: mô tả dấu hiệu CPU-vs-lag đã khoanh vùng nút thắt.
- **Dẫn dắt bằng fix trước khi chẩn đoán.** Người phỏng vấn muốn thấy tư duy của bạn. Kể chuyện chẩn đoán trước, rồi mới đến fix.

### 9. Red Flags
- "~4M trong vài tiếng" — sẵn sàng đặt con số vào bối cảnh (tốc độ produce × giờ) để nó nghe có đo đạc, không phải phóng đại.
- "hàng triệu xuống vài trăm" — làm rõ đây là lag *steady-state* sau fix, không phải bạn xoá 4M message; bạn *drain* chúng.
- Đảm bảo bạn không ngụ ý mình sửa một mình nếu người khác cùng triage — "em chẩn đoán root cause và đẩy fix" là an toàn.

### 10. Kịch Bản Chốt
"Một sự cố em hay nhắc: consumer lag trên pipeline dữ liệu giao dịch bùng nổ — khoảng bốn triệu message tụt lại
trong vài tiếng, và vẫn đang leo. Dấu hiệu là lag tăng trong khi CPU consumer không bão hoà, cái đó nói bạn đang
I/O-bound, đang chờ cái gì đó, không phải compute-bound. Root cause là xử lý per-message — mỗi message Kafka làm
round-trip Postgres và commit offset riêng, nên bọn em có trần cứng về message mỗi giây, và một khi produce vượt
nó, lag tăng mãi mãi. Bản năng là thêm consumer, nhưng cái đó chỉ trải rộng cùng chi phí per-message, mà bọn em
gần số partition rồi. Nên em thiết kế lại ingestion để batch — poll một batch, dedup và gom, ghi bulk vào Postgres,
một commit mỗi batch — cái đó xẹp chi phí chủ đạo. Steady-state lag đi từ hàng triệu xuống vài trăm và bọn em có
headroom throughput thật. Bài học chính là chẩn đoán trước khi scale; nút thắt là một hằng số nhân, không phải
thiếu parallelism."

---

## Bullet R4 — Service cảnh báo giá real-time + engine token trending (state machine, Redis pub/sub, Socket.IO, sorted sets)

> *"Built a real-time price-alert service (MongoDB) with an in-memory threshold-crossing state machine, Redis
> pub/sub for cross-pod coherence, and dual Socket.IO + Kafka delivery; plus a trending-token engine using
> Redis sorted sets across 5m/1h/6h/1d windows."*

### 1. Ý đồ (Intent)
Đây là bullet **hệ thống real-time + thiết kế cấu trúc dữ liệu Redis**. Nó cho thấy bạn xây được các tính năng
stateful, low-latency chạy đúng qua nhiều pod (phần khó của "real-time" trong một thế giới scale ngang). Ấn tượng:
*người này hiểu tính nhất quán state qua các replica, chọn đúng primitive Redis cho việc, và nghĩ về ngữ nghĩa
delivery.*

### 2. Elevator Pitch (20–30 giây)
"Em xây service cảnh báo giá real-time. Người dùng đặt ngưỡng — 'báo cho tôi khi token này vượt X' — và em giữ một
state machine trong bộ nhớ theo dõi mỗi cảnh báo đang trên hay dưới ngưỡng, nên em chỉ bắn khi thật sự cắt ngưỡng,
không phải mỗi tick giá. Chỗ hóc búa là bọn em chạy nhiều pod, nên em dùng Redis pub/sub để giữ state đó nhất quán
qua các pod, và delivery đi ra qua cả Socket.IO lẫn Kafka. Em cũng xây engine token trending trên Redis sorted set."

### 3. Bản Sâu (1–2 phút)
"Tính năng cảnh báo giá nghe đơn giản — báo tôi khi một token cắt một mức giá — nhưng các ràng buộc thú vị là
'real-time' và 'nhiều pod'.

Ngây thơ thì bạn re-check mọi cảnh báo trên mỗi tick giá và bắn nếu price > threshold, nhưng cái đó spam người dùng:
nếu một token dao động quanh ngưỡng bạn sẽ bắn liên tục. Nên phần lõi là một state machine cắt ngưỡng: với mỗi cảnh
báo em theo dõi một state — trên hay dưới — và em chỉ phát một sự kiện khi có chuyển từ dưới lên trên hoặc ngược lại.
Cái đó biến 'báo khi cắt' thành một thứ edge-triggered sạch sẽ thay vì level-triggered.

Em giữ state đó trong bộ nhớ để có latency — bạn không muốn một lần đọc DB mỗi tick. Nhưng bọn em chạy nhiều pod, và
một cập nhật giá có thể tới bất kỳ pod nào, nên mỗi pod có bản state riêng sẽ nghĩa là bắn không nhất quán — pod này
nghĩ đang trên, pod kia nghĩ đang dưới. Cách sửa của em là Redis pub/sub: các chuyển state và cập nhật giá được
publish để mọi pod hội tụ về cùng một góc nhìn, nên bất kỳ pod nào thấy một tick, việc cắt ngưỡng được đánh giá nhất
quán. MongoDB là store bền cho bản thân định nghĩa cảnh báo.

Với delivery em chọn dual: Socket.IO để push trực tiếp tới client đang kết nối, và Kafka cho mọi thứ khác cần phản
ứng khi một cảnh báo bắn — nên sự kiện cảnh báo không bị kẹt trong tầng websocket, các service khác cũng consume
được. Sự tách rời đó quan trọng vì 'người dùng thấy một toast' và 'các hệ thống downstream biết một cảnh báo đã bắn'
là hai mối quan tâm khác nhau.

Engine token trending là một mảnh riêng nhưng liên quan: em dùng Redis sorted set key theo hoạt động, với nhiều
cửa sổ thời gian — 5 phút, 1 giờ, 6 giờ, 1 ngày. Sorted set hoàn hảo ở đây vì 'top N trending' đúng là một ranked
range query, tức O(log n) — em có ranking gần như miễn phí thay vì sort trong app. Các cửa sổ cho phép UI hiển thị
trending theo nhiều khoảng thời gian.

Kết quả: cảnh báo bắn một lần đúng lúc cắt, nhất quán qua các pod, và ranking rẻ để query. Bài học chủ yếu là về
chọn đúng primitive — một state machine để edge-trigger, pub/sub để nhất quán, sorted set để ranking — thay vì brute-force
bất kỳ cái nào trong code ứng dụng."

### 4. Interview Hooks
- "state machine cắt ngưỡng trong bộ nhớ" → *Vì sao trong bộ nhớ? Các state nào? Vì sao không chỉ price > threshold?*
- "Redis pub/sub cho cross-pod coherence" → *Nhiều pod thì hỏng gì? Vì sao pub/sub thay vì shared store?*
- "dual Socket.IO + Kafka delivery" → *Vì sao cả hai? Khác nhau về mục đích?*
- "Redis sorted set qua cửa sổ 5m/1h/6h/1d" → *Duy trì cửa sổ thế nào? Hết hạn? Vì sao sorted set?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Tính năng cảnh báo giá làm gì từ góc nhìn người dùng?
2. (L1) Vì sao state machine thay vì check price > threshold mỗi lần?
3. (L2) Vì sao giữ state cảnh báo trong bộ nhớ thay vì thẳng trong Redis/Mongo?
4. (L2) Redis pub/sub điều phối chính xác cái gì ở đây?
5. (L2) Vì sao deliver qua cả Socket.IO lẫn Kafka?
6. (L3) State machine cấu trúc thế nào — state, transition, trigger?
7. (L3) Bạn duy trì các cửa sổ 5m/1h/6h/1d với sorted set thế nào?
8. (L3) Bạn giữ sorted set khỏi phình vô hạn thế nào (hết hạn/trim)?
9. (L4) Một pod restart và mất state trong bộ nhớ — chuyện gì xảy ra với cảnh báo?
10. (L4) Redis pub/sub là fire-and-forget — nếu một pod bỏ lỡ một message thì sao?
11. (L4) Cảnh báo bắn trùng — làm sao tránh báo cho một user hai lần?
12. (L5) In-memory + pub/sub vs một shared Redis state duy nhất — trade-off?
13. (L5) Sorted set vs một time-series DB cho trending — vì sao sorted set?
14. (L6) Người dùng báo cảnh báo bị lỡ/trễ trên prod — bạn debug thế nào?
15. (L6) Một glitch price feed gửi một spike rồi correction — làm sao tránh cảnh báo giả?
16. (L7) Hàng triệu cảnh báo active qua nhiều token — state trong bộ nhớ còn scale không?
17. (L7) Sorted set cho một token nóng phình to — giữ query cửa sổ nhanh thế nào?
18. (L8) Redis Streams hay Kafka có tốt hơn pub/sub cho tính nhất quán không?
19. (L8) Có thể làm trending bằng một windowed stream processor không? Vì sao/không?
20. (L8) Bạn làm delivery cảnh báo exactly-once tới người dùng thế nào?

### 6. Câu Trả Lời Chuẩn
1. "Người dùng đặt một quy tắc — 'báo tôi khi TOKEN lên/xuống X' — và nhận một push real-time đúng khoảnh khắc nó thực sự cắt, không phải liên tục khi nó dao động."
2. "Vì 'price > threshold' là level-triggered — nó đúng ở mọi tick trên đường, nên bạn sẽ spam. Một state machine là edge-triggered: nó chỉ bắn ở chuyển từ dưới lên trên, đúng cái người dùng thực sự hiểu là 'đã cắt'."
3. "Latency. Cảnh báo được đánh giá trên mỗi tick giá, và một lần đọc DB mỗi tick mỗi cảnh báo sẽ giết throughput. State trong bộ nhớ biến đường nóng thành một phép so sánh, không phải I/O — Mongo giữ định nghĩa bền, bộ nhớ giữ state sống."
4. "Tính nhất quán qua các pod. Bất kỳ pod nào cũng có thể nhận một tick giá cho trước, nên pub/sub broadcast các chuyển state/cập nhật giá để mọi pod đánh giá việc cắt dựa trên cùng góc nhìn — nếu không hai pod bất đồng về trên/dưới và bắn không nhất quán."
5. "Mục đích khác nhau. Socket.IO là push trực tiếp tới người dùng đang kết nối; Kafka để sự kiện cảnh-báo-đã-bắn có sẵn cho các service khác (analytics, thông báo khác) thay vì bị kẹt trong tầng websocket. Tách rời 'sự kiện' khỏi 'push'."
6. "Hai state mỗi cảnh báo — TRÊN và DƯỚI — với trigger là một tick giá. Chuyển DƯỚI→TRÊN (hoặc ngược lại) phát một sự kiện bắn; ở lại cùng state thì không phát gì. Đó là toàn bộ edge-trigger."
7. "Mỗi cửa sổ là một sorted set score theo hoạt động trong khoảng đó; em thêm/tăng score khi sự kiện tới và dùng ranked range query cho top-N. Các cửa sổ được duy trì bằng cách scope/hết hạn entry theo khoảng thời gian của nó."
8. "Trim/hết hạn — entry cũ rơi khỏi cửa sổ nên set chỉ phản ánh khoảng đó (ví dụ 5 phút gần nhất). Không có nó set phình mãi và 'trending' hết nghĩa 'gần đây'. Nên mỗi cửa sổ bị giới hạn bởi scope thời gian của nó."
9. "Nó rebuild — định nghĩa bền nằm trong Mongo và giá hiện tại biết được, nên khi restart pod tái suy ra state trên/dưới của mỗi cảnh báo từ giá hiện tại trước khi bắt đầu bắn. Nên restart không bắn cảnh báo giả."
10. "Việc pub/sub lossy chính là lý do state tái suy ra được — một message bị lỡ không thể làm desync vĩnh viễn vì state được đối chiếu lại từ định nghĩa bền + giá hiện tại. Em coi pub/sub là một optimization cho độ tươi, không phải source of truth."
11. "Edge-trigger đã ngăn bắn lặp khi điều kiện còn đúng, và em dedup ở phía delivery theo khoá (cảnh báo, sự kiện cắt) để dù Kafka at-least-once thì một user cũng không bị báo hai lần cho cùng một lần cắt."
12. "Một shared Redis state duy nhất đơn giản hơn và luôn nhất quán nhưng đặt một round-trip Redis lên đường nóng mỗi tick. In-memory + pub/sub giữ đường nóng cục bộ để có latency và chỉ dùng pub/sub để sync — bạn đổi một chút phức tạp lấy chi phí per-tick thấp hơn nhiều."
13. "TSDB tuyệt cho query lịch sử, nhưng 'top N ngay bây giờ' là một ranking query, và sorted set làm cái đó trong O(log n) một cách native không cần scan. Cho leaderboard sống, sorted set là primitive đúng mục đích; TSDB sẽ là over-engineering query nóng."
14. "Trace một cảnh báo từ đầu tới cuối: tick giá có tới không, chuyển state có được tính không, pub/sub có lan không, Socket.IO/Kafka có deliver không. Lỡ vs trễ thường tách ở 'việc cắt có được phát hiện không' vs 'delivery có trễ không' — log state machine làm cái đó tách được."
15. "State machine giúp vì spike-rồi-correction là hai chuyển; em có thể debounce hoặc yêu cầu việc cắt tồn tại một chút để một glitch một-tick không bắn. Tốt hơn là thêm một xác nhận nhỏ còn hơn báo trên nhiễu."
16. "In-memory scale tới khi bộ nhớ mỗi pod là giới hạn; rồi bạn shard cảnh báo qua các pod theo token/user để mỗi pod giữ một tập con, và pub/sub chỉ giữ tập con liên quan nhất quán. State mỗi cảnh báo rất nhỏ, nên trần rất cao trước khi cần shard."
17. "Giữ set giới hạn theo cửa sổ bằng trim, và cho top-N bạn chỉ cần đầu set, vốn giữ O(log n) bất kể tổng kích thước. Nếu tốc độ event thô của một token khổng lồ, aggregate trước khi ghi (bucket theo khoảng) để set không nhận mọi tick."
18. "Redis Streams/Kafka cho durability và replay mà pub/sub không có — tốt hơn nếu tính nhất quán phải sống sót qua mất message. Em dùng pub/sub có chủ đích vì state tái suy ra được từ store bền, nên em không cần nhất quán bền, chỉ cần nhất quán nhanh. Nếu tái suy ra đắt, em sẽ chuyển sang Streams."
19. "Được — một windowed stream processor tính trending một cách native. Em chọn sorted set vì chúng kiêm luôn đường đọc low-latency mà UI query trực tiếp; một stream processor vẫn cần chỗ nào đó để serve top-N, mà cái đó là... một sorted set. Nên em giữ nó trong một hệ."
20. "Exactly-once thật sự tới một user là khó vì hop cuối (mạng tới client) luôn có thể hỏng. Thực tế: delivery idempotent theo khoá sự kiện cắt + dedup phía client, để gửi lại vô hại. Em nhắm effectively-once mà người dùng cảm nhận thay vì exactly-once thật sự."

### 7. Whiteboard Version
Vẽ **price feed → các pod đánh giá cảnh báo (nhiều pod)** trước, với mỗi pod giữ một hộp state trong bộ nhớ. Rồi
vẽ **Redis pub/sub** như một bus nối các pod và ghi nhãn "cross-pod coherence." Vẽ **MongoDB** dưới các pod là
"định nghĩa cảnh báo bền (source of truth để tái suy ra state)." Từ một pod, vẽ hai mũi tên đầu ra: **Socket.IO →
client** và **Kafka → các service khác**, ghi nhãn "push" vs "sự kiện". Riêng ra, vẽ **engine trending**: events →
Redis sorted set (vẽ 4 set xếp chồng ghi 5m/1h/6h/1d) → top-N range query → API. Người phỏng vấn ngắt ở "nhiều pod
— state nhất quán thế nào?" (pub/sub + state tái suy ra được) và "vì sao sorted set?" (O(log n) ranking) — cả hai
đều mạnh.

### 8. Lỗi Thường Gặp
- **"Em chỉ check giá có trên ngưỡng không."** Lộ ra là bạn sẽ spam người dùng. Tốt hơn: "Nó edge-triggered qua một state machine, nên chỉ bắn khi cắt."
- **"State trong bộ nhớ nên ổn."** Bỏ qua nhất quán nhiều pod và restart. Tốt hơn: "In-memory cho latency, nhất quán qua pub/sub, tái suy ra được từ Mongo khi restart."
- **"Sorted set vì Redis nhanh."** Nông. Tốt hơn: gọi tên thuộc tính — "top-N là một ranked range query, O(log n), đúng thứ sorted set sinh ra để làm."

### 9. Red Flags
- "cross-pod coherence" qua pub/sub — sẵn sàng thừa nhận pub/sub lossy và giải thích *vì sao ở đây không sao* (state tái suy ra được); nếu không nó nghe như một lỗ hổng tính đúng đắn.
- "real-time" — làm rõ là soft real-time (latency thấp, best-effort), không phải đảm bảo hard real-time.
- "dual Socket.IO + Kafka delivery" — phải nói được crisp *vì sao cả hai*; nếu nghe như dư thừa cho có, nó thành over-engineering.

### 10. Kịch Bản Chốt
"Em xây service cảnh báo giá real-time, và phần vui là nó đánh lừa bằng vẻ đơn giản. Người dùng đặt một ngưỡng, và
cách ngây thơ — bắn bất cứ khi nào giá trên nó — spam người ta, vì nó đúng ở mọi tick. Nên phần lõi là một state
machine edge-triggered: mỗi cảnh báo ở state trên hoặc dưới, và em chỉ phát khi thật sự cắt. Em giữ state đó trong
bộ nhớ vì nó được đánh giá trên mỗi tick giá và em không kham nổi một lần đọc DB mỗi tick. Điểm hóc búa là bọn em
chạy nhiều pod và bất kỳ pod nào cũng có thể nhận một tick, nên em dùng Redis pub/sub để giữ state nhất quán qua
chúng — và quan trọng là state tái suy ra được từ định nghĩa cảnh báo bền trong Mongo, nên việc pub/sub lossy không
gây desync vĩnh viễn. Delivery là dual: Socket.IO để push tới người dùng, Kafka để các service khác phản ứng khi
bắn mà không bị bó vào tầng websocket. Có một mảnh anh em — engine token trending trên Redis sorted set qua vài cửa
sổ thời gian, vì top-N trending chỉ là một ranked range query, thứ sorted set làm trong log n. Em sẵn lòng đào sâu
vào phần nhất quán hay phần cửa sổ."

---

# PHẦN 2 — VDAX (sàn giao dịch tập trung kiểu Binance)

---

## Bullet V1 — Engine hoa hồng/referral (tích luỹ nhiều tầng, dedup, xử lý tiền chính xác decimal)

> *"Built the referral reward/commission engine: multi-tier commission accrual on trades and settlements,
> deduplicated by a unique (user, transaction) constraint with decimal-precise money handling."*

### 1. Ý đồ (Intent)
Đây là bullet **tính đúng đắn về tiền ở một miền khác** — nó bổ trợ cho PnL bằng cách cho thấy bạn xử lý tích luỹ
tài chính, deduplication, và độ chính xác một cách tổng quát, không phải một lần cho vui. Hoa hồng nhiều tầng cũng
là tín hiệu "mô hình hoá đúng một business rule". Ấn tượng: *người này đáng tin giao code về tiền — họ nghĩ về
trả trùng, độ chính xác, và các ràng buộc bắt buộc tính đúng đắn ở tầng DB.*

### 2. Elevator Pitch (20–30 giây)
"Ở phía sàn em xây engine hoa hồng referral. Khi người bạn giới thiệu giao dịch hay settlement, bạn kiếm được hoa
hồng, và nó nhiều tầng — nên nó tích luỹ ngược lên chuỗi giới thiệu. Hai thứ em quan tâm nhất là không trả cho ai
hai lần và không mất độ chính xác của tiền — nên mỗi lần tích luỹ đều được dedup bằng một ràng buộc duy nhất
(user, transaction) ở tầng DB, và toàn bộ phép toán làm bằng kiểu decimal chính xác, không bao giờ float."

### 3. Bản Sâu (1–2 phút)
"Hệ thống referral là một tính năng tăng trưởng nhưng về bản chất là tiền, nên nó sống chết bằng tính đúng đắn. Quy
tắc là: user giới thiệu user khác, và khi một user được giới thiệu giao dịch hoặc có settlement, người giới thiệu
kiếm hoa hồng — và nó nhiều tầng, nên có thể tích luỹ ngược lên vài cấp của chuỗi giới thiệu, mỗi cấp một mức riêng.

Hai yêu cầu khó. Thứ nhất, idempotency: một sự kiện trade hoặc settlement có thể được gửi hơn một lần — cùng thực
tế at-least-once như phía trading — và nếu em tích luỹ hoa hồng hai lần cho cùng một transaction, em đang trả ra
tiền không được kiếm. Em bắt buộc điều đó bằng một ràng buộc duy nhất (user, transaction) trong Postgres, nên
*database* đảm bảo một user chỉ được credit một lần cho một transaction. Dù code của em cố insert một accrual trùng,
ràng buộc reject nó — tính đúng đắn được bắt buộc ở tầng lưu trữ, không chỉ ở logic ứng dụng, đó là chỗ em muốn các
đảm bảo về tiền nằm.

Thứ hai, độ chính xác: toán tiền với float là cách kinh điển làm rò rỉ hoặc bịa ra vài phần trăm cent. Nên mọi số
lượng và mức dùng kiểu decimal chính xác từ đầu tới cuối — hoa hồng là `amount × rate` tính bằng decimal, nên nó
chính xác và đối chiếu được. Điều đó gấp đôi quan trọng với nhiều tầng, nơi bạn áp nhiều mức và việc làm tròn phải
được định nghĩa, không phải ngẫu nhiên.

Thiết kế là làm accrual thành một phép append các bản ghi hoa hồng bất biến key theo (user, transaction), rồi số dư
phải trả được suy ra từ các bản ghi đó. Như vậy các bản ghi là audit trail và ràng buộc là dedup — em không phải tin
rằng bus sự kiện gửi đúng một lần.

Kết quả: hoa hồng đúng tới từng cent, không thể trả trùng, và audit được. Bài học củng cố cái em học từ PnL — với
tiền, đẩy đảm bảo tính đúng đắn càng gần tầng lưu trữ càng tốt, và đừng bao giờ tin delivery là exactly-once."

### 4. Interview Hooks
- "tích luỹ hoa hồng nhiều tầng" → *Bạn mô hình hoá tầng/chuỗi thế nào? Sâu tới đâu?*
- "dedup bằng ràng buộc duy nhất (user, transaction)" → *Vì sao ở tầng DB? Race condition thì sao?*
- "xử lý tiền chính xác decimal" → *Vì sao không float? Làm tròn thế nào?*
- "trên trades và settlements" → *Các sự kiện này đến từ đâu? At-least-once?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Hoa hồng referral hoạt động thế nào từ góc nhìn người dùng?
2. (L1) "Nhiều tầng" ở đây cụ thể nghĩa là gì?
3. (L2) Bạn mô hình hoá chuỗi giới thiệu và các tầng trong schema thế nào?
4. (L2) Các sự kiện trade/settlement đến từ đâu và delivery guarantee gì?
5. (L2) Vì sao bắt buộc dedup bằng ràng buộc DB thay vì trong code ứng dụng?
6. (L3) Idempotency key chính xác của bạn là gì và vì sao (user, transaction)?
7. (L3) Bạn tính hoa hồng với decimal thế nào — kiểu dữ liệu, quy tắc làm tròn?
8. (L3) Bạn xử lý rollup nhiều tầng thế nào — đệ quy, chuỗi tính trước, hay lặp?
9. (L4) Hai sự kiện cho cùng một transaction tới đồng thời — chuyện gì xảy ra ở DB?
10. (L4) Một trade sau đó bị đảo/huỷ — bạn claw back hoa hồng thế nào?
11. (L4) Insert hoa hồng thành công nhưng một bước downstream hỏng — nhất quán?
12. (L5) Bản ghi accrual bất biến + số dư suy ra vs một running balance có thể sửa — trade-off?
13. (L5) Vì sao decimal thay vì integer-cents (minor units)? Hay bạn dùng minor units?
14. (L6) Finance báo lệch payout — bạn đối chiếu/audit thế nào?
15. (L6) Bạn phát hiện một mức bị cấu hình sai suốt một tuần — sửa accrual lịch sử thế nào?
16. (L7) Chuỗi giới thiệu rất sâu và viral — rollup còn scale không?
17. (L7) Khối lượng trade cao — insert unique-constraint có thành hotspot không?
18. (L8) Bạn tính hoa hồng đồng bộ ngay trên trade hay async qua sự kiện? Vì sao?
19. (L8) Event-sourcing sổ hoa hồng vs thiết kế hiện tại — có đáng không?
20. (L8) Bạn hỗ trợ thay đổi quy tắc hoa hồng thế nào mà không làm hỏng accrual quá khứ?

### 6. Câu Trả Lời Chuẩn
1. "Nếu bạn giới thiệu ai đó và họ giao dịch, bạn kiếm một phần phí. Nó liên tục — mỗi trade hoặc settlement đủ điều kiện của người được giới thiệu tích luỹ hoa hồng cho bạn."
2. "Nó tích luỹ ngược lên hơn một cấp của cây giới thiệu — người giới thiệu trực tiếp được một mức, và các cấp trên có thể được mức nhỏ hơn. Nên một trade có thể sinh ra vài bản ghi hoa hồng ở các tầng khác nhau."
3. "Mỗi user có một con trỏ referrer tạo thành chuỗi; các tầng là các mức áp theo độ sâu. Để tích luỹ, em đi (hoặc tính trước) chuỗi từ user giao dịch đi lên và phát một bản ghi hoa hồng cho mỗi tầng áp dụng."
4. "Sự kiện trade/settlement đến từ pipeline sự kiện (kiểu Kafka), vốn at-least-once — nên gửi lại là điều được kỳ vọng và cả engine phải idempotent với nó."
5. "Vì check ở tầng ứng dụng bị race — hai handler đồng thời cùng có thể check 'chưa tích luỹ' và cùng insert. Một ràng buộc duy nhất là atomic ở DB; đó là chỗ duy nhất thật sự đảm bảo được once-only, nên tính đúng đắn về tiền nằm ở đó, không phải một check-then-write trong code có thể race."
6. "(user, transaction) — người thụ hưởng cộng transaction nguồn. Đó là tính duy nhất tự nhiên: một user được credit nhiều nhất một lần cho một transaction. Nó sống sót qua gửi lại và concurrency vì DB bắt buộc nó."
7. "Kiểu decimal từ đầu tới cuối; hoa hồng là amount × rate bằng decimal với quy tắc làm tròn tường minh (được định nghĩa, không phải hành vi float mặc định). Float sẽ đưa vào sai số biểu diễn cộng dồn qua các tầng và không đối chiếu được tới cent."
8. "Em giải chuỗi cho user giao dịch và lặp qua các tầng, phát một accrual cho mỗi tầng với mức của tầng đó. Làm per-tầng thành các bản ghi riêng key theo (user, transaction) nghĩa là mỗi tầng idempotent độc lập."
9. "Một insert thắng, cái kia đụng ràng buộc duy nhất và bị reject/catch như một bản trùng — mà em coi là thành công (đã tích luỹ), không phải lỗi. Đó chính là lý do ràng buộc tồn tại: nó biến một race thành một no-op."
10. "Một lần đảo là một bản ghi bù (compensating), không phải một phép sửa — em tích luỹ một hoa hồng âm/bù key theo transaction đảo. Số dư phải trả suy ra sẽ triệt tiêu chúng, và audit trail giữ cả hai, thứ finance cần."
11. "Insert accrual và state liên quan nằm trong một transaction khi có thể, nên nó all-or-nothing. Nếu một bước downstream thật sự tách rời hỏng, bản ghi accrual vẫn là source of truth và downstream có thể retry lại nó một cách idempotent."
12. "Bản ghi bất biến + số dư suy ra cho bạn audit trail đầy đủ và làm dedup/claw-back sạch (append các bản bù), đổi lại phải tính số dư. Một running balance có thể sửa thì rẻ để đọc nhưng mất lịch sử và khó đối chiếu hơn nhiều — với tiền em chọn audit trail."
13. "Điểm mấu chốt là số học cơ số 10 chính xác với làm tròn định nghĩa được; dù đó là kiểu decimal hay integer minor units, thứ cần tránh là float. Em dùng xử lý chính xác decimal để mức và số lượng nhân chính xác và làm tròn dự đoán được."
14. "Các bản ghi bất biến là audit trail — mỗi payout truy được về (user, transaction, tầng, mức, số tiền). Đối chiếu là tái suy ra số dư từ các bản ghi và so với cái đã trả; lệch khoanh về các bản ghi cụ thể."
15. "Vì accrual bất biến và có key, em không viết lại lịch sử — em phát các bản ghi sửa cho các transaction bị ảnh hưởng với delta giữa mức sai và đúng. Sổ vẫn append-only và audit được, và số dư suy ra tự sửa."
16. "Độ sâu chuỗi bị giới hạn bởi số tầng (bạn chỉ trả N cấp), nên chi phí rollup mỗi trade là O(tầng), không phải O(độ dài chuỗi) — đó là một cái cap có chủ đích. Nên viral làm tăng số trade, không phải chi phí mỗi trade, và cái đó scale theo consumer."
17. "Insert unique-constraint là một phép ghi có index đơn lẻ; nó rẻ. Dưới khối lượng rất cao, thứ cần theo dõi là index trên (user, transaction), và batch các accrual như đường PnL sẽ phân bổ đều nó — cùng một playbook."
18. "Async qua sự kiện. Hoa hồng không nên block hay làm chậm đường trade, và làm nó ngoài stream sự kiện cho em retry/replay và tách các tính năng tăng trưởng khỏi luồng trading quan trọng. Trade hoàn tất; hoa hồng tích luỹ sau, một cách idempotent."
19. "Thiết kế đã là event-sourcing-lite rồi — các bản ghi accrual bất biến là cái sổ. Event-sourcing đầy đủ (log sự kiện replay được là source of truth duy nhất) sẽ thêm sức mạnh cho audit/replay nhưng thêm máy móc; với cái này thì sổ bản-ghi-có-key trúng điểm ngọt."
20. "Quy tắc được version và áp tại thời điểm accrual, và vì bản ghi bất biến với mức lưu trên nó, đổi quy tắc về sau không bao giờ đụng accrual quá khứ. Lịch sử phản ánh quy tắc có hiệu lực lúc đó — đúng cái finance và người dùng kỳ vọng."

### 7. Whiteboard Version
Vẽ **cây giới thiệu** trước (một user với một chuỗi referrer phía trên) để các tầng trực quan. Rồi vẽ một **sự kiện
trade/settlement** đến từ Kafka vào **engine hoa hồng**. Bên trong, cho thấy `giải chuỗi → accrual mỗi tầng →
insert`, và vẽ **bảng bản ghi hoa hồng** với một nhãn lớn trên ràng buộc duy nhất `(user, transaction)` — khoanh
tròn nó và nói "đây là chỗ tính đúng đắn nằm." Cho thấy **số dư phải trả** là *suy ra* từ các bản ghi (một mũi tên,
không phải một cột). Người phỏng vấn ngắt ở "nếu sự kiện được gửi hai lần thì sao" (ràng buộc biến thành no-op) và
"float vs decimal" (decimal, làm tròn định nghĩa được). Nếu được hỏi về đảo, thêm một bản ghi âm bù.

### 8. Lỗi Thường Gặp
- **"Em check xem nó tồn tại chưa, rồi insert."** Bug race kinh điển. Tốt hơn: "Ràng buộc duy nhất làm nó atomic; một insert trùng là no-op, nên em không dựa vào check-then-write."
- **"Em dùng float cho các số tiền."** Red flag về tiền ngay lập tức. Tốt hơn: "Decimal từ đầu tới cuối với quy tắc làm tròn định nghĩa được."
- **"Em update một running balance."** Mở đường cho các câu hỏi 'audit / claw back thế nào' bạn sẽ chật vật. Tốt hơn: "Append bản ghi bất biến, suy ra số dư."

### 9. Red Flags
- "nhiều tầng" — sẵn sàng nói cái cap tầng; "chuỗi vô hạn" nghe như một O(n) vô tình hoặc một exploit payout. "Bọn em trả N cấp cố định" thì dễ bảo vệ.
- "chính xác decimal" — biết quy tắc làm tròn của bạn và nó xảy ra ở đâu; mơ hồ ở đây nghe như "chưa thật sự ship code về tiền."
- Đừng nói quá scope trên VDAX — "em xây engine referral" thì được; "em xây cả sàn" thì không.

### 10. Kịch Bản Chốt
"Trên sàn em own engine hoa hồng referral. Quy tắc là nhiều tầng — khi người bạn giới thiệu giao dịch hay settlement,
bạn kiếm hoa hồng, và nó tích luỹ ngược lên một số cấp cố định của chuỗi giới thiệu, mỗi cấp một mức riêng. Vì là
tiền, hai thứ chi phối thiết kế. Một, idempotency: sự kiện trade là at-least-once, nên em bắt buộc dedup bằng một
ràng buộc duy nhất (user, transaction) trong Postgres — một lần gửi trùng chỉ đụng ràng buộc và thành no-op, nên
tính đúng đắn được đảm bảo ở tầng lưu trữ, không phải trong một check-then-write có thể race. Hai, độ chính xác:
toàn bộ toán là decimal từ đầu tới cuối với quy tắc làm tròn định nghĩa được, không bao giờ float, nên hoa hồng đối
chiếu tới cent kể cả qua các tầng. Em mô hình hoá accrual thành các bản ghi bất biến key theo (user, transaction)
và suy ra số dư phải trả từ chúng, cái đó cũng làm việc đảo sạch — một claw-back chỉ là một bản ghi bù. Em sẵn lòng
đi vào phần xử lý đảo hay cách em đổi mức mà không làm hỏng lịch sử."

---

## Bullet V2 — Pipeline thông báo (Kafka worker, RabbitMQ email, Redis Socket.IO, Firebase push, idempotent)

> *"Owned the notification pipeline via Kafka background workers — RabbitMQ transactional emails, Redis-backed
> Socket.IO real-time updates, and Firebase push — with idempotent delivery across deposit/withdrawal settlement events."*

### 1. Ý đồ (Intent)
Đây là bullet **fan-out / delivery đa kênh + idempotency**. Nó cho thấy bạn xây được một pipeline nhận một sự kiện
miền và fan-out nó đáng tin qua các kênh khác loại mà không báo trùng — đó là một bài toán hệ phân tán thật sự khoác
áo "thông báo". Ấn tượng: *người này thiết kế hệ thống delivery với đúng đảm bảo cho từng kênh và hiểu idempotency
qua một fan-out.*

### 2. Elevator Pitch (20–30 giây)
"Em own pipeline thông báo trên sàn. Các sự kiện miền — chủ yếu là settlement nạp và rút — đến qua Kafka, và các
background worker fan-out chúng qua ba kênh: email giao dịch qua RabbitMQ, cập nhật in-app real-time qua Socket.IO
có Redis đằng sau, và push mobile qua Firebase. Thứ em quan tâm nhất là delivery idempotent — sự kiện settlement là
sự kiện tiền và có thể được gửi lại, nên em đảm bảo một user không bị báo hai lần cho cùng một settlement."

### 3. Bản Sâu (1–2 phút)
"Pipeline thông báo là một bài toán fan-out. Một sự kiện miền đơn lẻ — ví dụ một lần rút settle — cần tới người dùng
qua vài kênh, mỗi kênh có đặc tính khác nhau. Email là giao dịch và có thể chậm hơn; in-app cần real-time; push đi
qua một bên thứ ba. Nên thiết kế là: consume sự kiện một lần từ Kafka trong một background worker, rồi dispatch tới
các kênh.

Em cố ý giữ các kênh tách rời sau transport riêng của chúng. Email đi qua RabbitMQ — nó là một work queue tự nhiên
cho email giao dịch với retry, và nó cô lập một provider email chậm hoặc chập chờn khỏi phần còn lại của pipeline.
In-app real-time đi qua Socket.IO có Redis đằng sau, để nó chạy qua nhiều pod — cùng mối lo nhất quán như service
cảnh báo, Redis là thứ cho phép bất kỳ pod nào deliver tới connection của một user. Push đi qua Firebase. Giữ chúng
tách rời nghĩa là một kênh hỏng — ví dụ Firebase down — không block email hay in-app.

Yêu cầu tính đúng đắn là idempotency, và nó quan trọng hơn ở đây vì đây là sự kiện settlement — tiền đã chuyển. Kafka
là at-least-once, nên cùng một sự kiện settlement có thể tới hai lần, và 'bạn đã rút 500$' hai lần thì đáng báo động
với người dùng và làm xói mòn niềm tin. Nên delivery được key theo id sự kiện settlement cho từng kênh — em theo dõi
cái gì đã được deliver để gửi lại bị chặn. Idempotency là per-kênh vì 'đã gửi email' và 'đã gửi push' là hai sự thật
độc lập.

Phần 'background worker' cũng quan trọng — thông báo nằm ngoài đường settlement quan trọng, nên một provider email
chậm không bao giờ làm chậm xử lý nạp/rút thực tế. Settlement hoàn tất; thông báo xảy ra async và đáng tin.

Kết quả: thông báo đa kênh đáng tin nơi các kênh hỏng độc lập và người dùng không bao giờ nhận thông báo tiền trùng.
Bài học là 'thông báo' thật ra là một hệ thống delivery fan-out, và phần kỹ thuật thú vị là đảm bảo per-kênh và
idempotency, không phải bản thân các message."

### 4. Interview Hooks
- "Kafka background worker" → *Vì sao background/async? Cái gì trên đường quan trọng vs không?*
- "RabbitMQ transactional email" → *Vì sao RabbitMQ cho email khi đã có Kafka?*
- "Socket.IO có Redis đằng sau" → *Vì sao Redis đằng sau Socket.IO? Nhiều pod?*
- "delivery idempotent qua sự kiện settlement nạp/rút" → *Bạn dedup qua ba kênh thế nào?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Pipeline này gửi những loại thông báo nào?
2. (L1) Vì sao gửi thông báo async qua background worker?
3. (L2) Vì sao ba kênh/transport riêng thay vì một?
4. (L2) Vì sao RabbitMQ cho email khi Kafka đã ở đó?
5. (L2) Vì sao cần Redis đằng sau Socket.IO?
6. (L3) Bạn làm delivery idempotent qua các kênh chính xác thế nào?
7. (L3) Idempotency là per-kênh hay global per sự kiện — và vì sao?
8. (L3) Bạn theo dõi cái gì đã deliver thế nào — state đó ở đâu?
9. (L4) Firebase down một giờ — chuyện gì xảy ra với push?
10. (L4) Provider email chấp nhận rồi âm thầm drop — làm sao bạn biết/retry?
11. (L4) Một worker crash giữa fan-out (email đã gửi, push chưa) — làm sao tránh gửi lại email?
12. (L5) At-least-once + idempotency vs cố exactly-once — trade-off ở đây?
13. (L5) Vì sao fan-out trong một worker vs một consumer riêng cho mỗi kênh?
14. (L6) Người dùng báo email "rút hoàn tất" trùng — bạn debug thế nào?
15. (L6) Backlog hình thành trên email queue — bạn xử lý ưu tiên thế nào (tiền vs marketing)?
16. (L7) Khối lượng thông báo tăng 10x trong một sự kiện thị trường — nó hỏng ở đâu?
17. (L7) Hàng triệu connection Socket.IO đồng thời — delivery có Redis đằng sau scale thế nào?
18. (L8) Bạn có dùng Kafka cho mọi kênh (bỏ RabbitMQ) không? Vì sao/không?
19. (L8) Outbox pattern để đảm bảo sự kiện đã được publish — bạn có thêm không?
20. (L8) Bạn hỗ trợ preference thông báo của user (opt-out per kênh) một cách sạch thế nào?

### 6. Câu Trả Lời Chuẩn
1. "Chủ yếu là sự kiện tài khoản/tiền — settlement nạp và rút — cộng các sự kiện user khác, deliver dưới dạng email, in-app real-time, và push mobile."
2. "Vì thông báo không được làm chậm hay gây nguy cho đường settlement. Settlement hoàn tất và commit; worker nhặt sự kiện sau đó. Một provider email chập chờn không bao giờ được phép làm trễ một lần rút của ai đó."
3. "Mỗi kênh có latency, failure mode, và provider khác nhau. Transport riêng cô lập chúng — email chậm không thể block in-app, Firebase down không thể block email. Ghép chúng lại sẽ làm cả pipeline chỉ đáng tin bằng kênh tệ nhất."
4. "Kafka là xương sống sự kiện miền; RabbitMQ là work queue per-kênh cho email với ngữ nghĩa retry/DLQ dễ và cô lập provider. Dùng RabbitMQ cho nhánh email giữ cho retry email khỏi replay cả sự kiện Kafka và tách các mối quan tâm."
5. "Socket.IO giữ connection trên các pod cụ thể, nhưng sự kiện có thể được consume bởi bất kỳ worker nào. Redis (adapter/pub-sub của Socket.IO) cho phép một worker deliver tới socket của một user bất kể pod nào đang giữ nó — không có Redis, real-time chỉ chạy nếu bạn tình cờ ở đúng pod."
6. "Delivery được key theo id sự kiện settlement cho từng kênh; trước khi gửi em check/ghi rằng (sự kiện, kênh) đó chưa được deliver. Gửi lại cùng sự kiện thấy nó đã gửi và bỏ qua — nên Kafka at-least-once không thành thông báo at-least-twice."
7. "Per-kênh. 'Đã gửi email' và 'đã gửi push' là hai sự thật độc lập — nếu email thành công nhưng push hỏng, em muốn retry chỉ push, không gửi lại email. Một key global sẽ ép all-or-nothing và gây gửi lại các kênh đã thành công."
8. "Trong một store theo dõi delivery key theo (sự kiện, kênh) — đủ bền để một worker restart thấy các lần deliver trước. Store đó là thứ làm fan-out an toàn dưới retry và crash."
9. "Push retry với backoff qua kênh của nó; các kênh khác không bị ảnh hưởng vì chúng tách rời. Khi Firebase hồi phục, các sự kiện push chưa deliver được retry, và idempotency key chặn các cái đã deliver khỏi gửi trùng."
10. "Drop âm thầm là case khó — em dựa vào tín hiệu delivery/ack của provider khi có và coi thiếu xác nhận là retryable, với idempotency chặn gửi trùng nếu thật ra nó đã đi. Nơi provider không cho tín hiệu, em giới hạn retry và log để đối chiếu."
11. "Bản ghi delivery per-kênh được ghi khi từng kênh thành công, nên khi restart worker thấy email đã xong và chỉ retry push. Cái bookkeeping per-kênh đó chính xác là thứ ngăn gửi lại kênh đã đi."
12. "Exactly-once thật sự qua các provider bên thứ ba là bất khả — hop provider luôn có thể hỏng sau khi ack. Nên at-least-once + idempotency key là lựa chọn thực tế và đúng: gửi lại vô hại, không trùng mà người dùng cảm nhận được."
13. "Consumer riêng cho mỗi kênh cũng hợp lệ và cho scale độc lập; em fan-out trong một worker với idempotency per-kênh cho đơn giản việc điều phối 'một sự kiện, ba lần deliver'. Nếu một kênh cần scale rất khác, tách nó thành consumer riêng là bước tiếp theo tự nhiên."
14. "Check store theo dõi delivery cho sự kiện/kênh đó — nếu có hai bản ghi delivery, idempotency key không được áp hoặc key sai; nếu có một nhưng hai email, cái trùng nằm trong provider email/retry RabbitMQ. Cái đó tách nhanh 'bug của mình' khỏi 'bug provider'."
15. "Queue/priority riêng — thông báo tiền giao dịch đi đường ưu tiên cao, marketing đường best-effort, nên backlog marketing không bao giờ làm trễ một xác nhận rút. Các loại thông báo khác nhau được các đảm bảo khác nhau."
16. "Queue hấp thụ spike (đó là việc của nó), nên giới hạn thật đầu tiên là throughput/rate limit của provider và capacity connection Socket.IO. Em sẽ back-pressure qua queue và scale worker/connection; thông báo tiền được ưu tiên để chúng vẫn kịp thời."
17. "Socket.IO scale ngang với adapter Redis phân phối message qua các pod; giới hạn thành throughput pub-sub Redis và số connection mỗi pod. Bạn shard connection qua nhiều pod hơn và có thể partition fan-out Redis nếu nó nóng."
18. "Có thể, nhưng ngữ nghĩa work-queue per-message của RabbitMQ (ack/nack, DLQ, retry) hợp hình dạng 'làm việc này, retry nếu hỏng' của email hơn mô hình log của Kafka. Em giữ Kafka cho sự kiện miền và RabbitMQ cho task queue email vì mỗi tool hợp việc của nó."
19. "Có — một outbox sẽ làm 'settlement đã xảy ra' và 'sự kiện thông báo đã được publish' atomic với DB commit, đóng cái khe nơi một settlement commit nhưng sự kiện không bao giờ publish. Đó là phần hardening đúng nếu bọn em thấy thông báo bị mất; một bổ sung sạch cho thiết kế hiện tại."
20. "Preference như một check trước mỗi lần dispatch kênh — worker tham vấn opt-in per-kênh của user trước khi gửi, nên một opt-out đơn giản là bỏ qua delivery của kênh đó (và bản ghi idempotency của nó). Giữ logic preference ra khỏi bản thân các transport kênh."

### 7. Whiteboard Version
Vẽ **sự kiện settlement trên Kafka** đi vào một hộp **notification worker**. Từ worker, vẽ **ba mũi tên toả ra** tới
ba kênh: `→ RabbitMQ → email`, `→ Redis → Socket.IO → in-app`, `→ Firebase → push`. Dưới worker, vẽ **store theo
dõi delivery** key theo `(sự kiện, kênh)` và ghi nhãn "idempotency — per kênh." Nhấn rằng worker nằm **ngoài đường
settlement quan trọng** (vẽ settlement commit vào DB của nó trước, rồi mới phát sự kiện). Người phỏng vấn ngắt ở "vì
sao cả RabbitMQ và Kafka" (log miền vs task queue) và "làm sao không báo trùng" (idempotency key per-kênh). Nếu bị
đẩy, thêm outbox pattern để đóng khe publish.

### 8. Lỗi Thường Gặp
- **"Kafka exactly-once nên không trùng."** Sai và đây là tiền. Tốt hơn: "at-least-once, nên em key delivery per-kênh để làm nó idempotent."
- **"Một idempotency key cho cả sự kiện."** Gây gửi lại các kênh đã thành công khi retry. Tốt hơn: "key per-kênh để chỉ retry cái nào hỏng."
- **"Thông báo đơn giản, chỉ gửi một email."** Đánh giá thấp nó. Tốt hơn: khung nó thành một fan-out đa kênh đáng tin với các failure domain độc lập.

### 9. Red Flags
- "delivery idempotent" — thành thật rằng hop cuối tới một provider không thể exactly-once; nói "idempotent từ phía bọn em / effectively-once tới người dùng", không phải "đảm bảo exactly-once".
- Dùng cả Kafka và RabbitMQ có thể nghe như phức tạp vô tình — luôn ghép với lý do "log vs task-queue" để nó nghe có chủ đích.
- "own pipeline thông báo" thì được; đừng ngụ ý bạn xây hạ tầng email/push từ đầu — bạn tích hợp provider.

### 10. Kịch Bản Chốt
"Em own pipeline thông báo trên sàn, thật ra là một bài toán fan-out delivery. Các sự kiện miền — chủ yếu settlement
nạp và rút — đến từ Kafka vào các background worker, và em dispatch chúng qua ba kênh: email giao dịch qua RabbitMQ,
in-app real-time qua Socket.IO có Redis đằng sau để chạy qua nhiều pod, và push mobile qua Firebase. Em giữ các kênh
tách rời sau transport riêng để một cái hỏng — ví dụ Firebase down — không thể block các cái kia. Phần tính đúng đắn
em quan tâm là idempotency, và nó per-kênh: sự kiện settlement là sự kiện tiền và Kafka là at-least-once, nên em key
delivery theo (sự kiện, kênh) — một settlement gửi lại thấy nó đã được email và bỏ qua, nhưng nếu push đã hỏng em chỉ
retry push. Và tất cả nằm ngoài đường settlement quan trọng, nên một provider email chậm không bao giờ làm trễ một
lần rút thực tế. Nếu hữu ích em có thể nói về cách thêm một outbox để đóng khe publish, hay lựa chọn RabbitMQ vs Kafka."

---

## Bullet V3 — Service tài khoản (vòng đời user, Sumsub KYC, nạp/rút đa chuỗi, TOTP 2FA)

> *"Contributed to the accounts service: user lifecycle, Sumsub KYC, and multi-chain deposits/withdrawals with
> TOTP 2FA over PostgreSQL."*

### 1. Ý đồ (Intent)
Đây là bullet **độ rộng + có ý thức bảo mật/tuân thủ**. Nó cho thấy bạn đã làm việc trên phần lõi nhạy cảm của một
sàn — danh tính, KYC, di chuyển tiền, 2FA — nơi bảo mật và tính đúng đắn là không thể thương lượng. Lưu ý nó ghi
"contributed", nên nó báo hiệu hợp tác và độ rộng chứ không phải sở hữu một mình. Ấn tượng: *người này làm việc được
trên các hệ thống nhạy về bảo mật và tuân thủ một cách có trách nhiệm và tích hợp bên thứ ba (KYC) đúng cách.*

### 2. Elevator Pitch (20–30 giây)
"Em cũng đóng góp vào service tài khoản, đó là phần lõi nhạy cảm của sàn — vòng đời user, KYC qua Sumsub, nạp và rút
đa chuỗi, và 2FA kiểu TOTP, tất cả trên Postgres. Đó là phần mà bảo mật và tính đúng đắn thực sự không được sai, nên
nhiều việc là tích hợp KYC đúng cách, xử lý luồng rút an toàn, và làm 2FA cho chuẩn."

### 3. Bản Sâu (1–2 phút)
"Service tài khoản là lõi danh tính và tiền của sàn, và nó là một service chung, rủi ro cao — nên em đóng góp vào nó
cùng team chứ không own một mình, đó là cách nói thành thật.

Các phần em đụng: vòng đời user — đăng ký, state, hành trình của tài khoản. KYC qua Sumsub — đó là một tích hợp
xác minh danh tính bên thứ ba, và phần thú vị là nó async và webhook-driven: bạn kích hoạt xác minh, provider làm
việc của họ, và bạn đối chiếu kết quả qua callback, nên bạn phải xử lý các state pending/approved/rejected và không
tin vào một response đơn lẻ. Nạp và rút đa chuỗi — di chuyển tiền thật qua các chuỗi, luồng nhạy về tính-đúng-đắn-và-bảo-mật
nhất trong cả sản phẩm, vì một bug ở đó là tiền rời đi. Và TOTP 2FA — mật khẩu dùng-một-lần theo thời gian bảo vệ các
hành động nhạy cảm như rút.

Ràng buộc xuyên suốt là đây là tiền và danh tính, nên chuẩn khác với một service CRUD thường. Với rút, trọng tâm thiết
kế là gating: yêu cầu 2FA, các chuyển state không thể bỏ qua xác minh, và idempotency để một yêu cầu rút bị retry không
thể double-spend. Với KYC, là xử lý provider async đúng cách — coi webhook của họ là source of truth và bền bỉ với
callback trùng hoặc sai thứ tự. Postgres là store vì dữ liệu này cần tính nhất quán mạnh và ràng buộc — bạn muốn các
đảm bảo quan hệ cho dữ liệu tài khoản và cận-số-dư.

Kết quả: em góp phần xây và làm cứng các luồng gating danh tính và tiền. Bài học, khi làm việc này, là bao nhiêu phần
của 'bảo mật' thực ra là state machine cẩn thận và idempotency — phần lớn sự an toàn đến từ không để hệ thống rơi vào
trạng thái xấu, không phải từ mật mã một mình."

### 4. Interview Hooks
- "Sumsub KYC" → *Bạn tích hợp một provider KYC async thế nào? Webhook? Idempotency?*
- "nạp/rút đa chuỗi" → *Bạn làm rút an toàn thế nào? Double-spend? Confirmation?*
- "TOTP 2FA" → *TOTP hoạt động thế nào? Bạn lưu secret ở đâu? Replay?*
- "vòng đời user trên PostgreSQL" → *Vì sao Postgres? Các chuyển state nào?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Service tài khoản sở hữu cái gì?
2. (L1) TOTP là gì và vì sao 2FA trên rút?
3. (L2) Bạn tích hợp một provider async như Sumsub thế nào — request/response hay webhook?
4. (L2) Vì sao Postgres cho service này cụ thể?
5. (L2) Các state chính trong luồng KYC và rút là gì?
6. (L3) Bạn lưu secret TOTP ở đâu và an toàn thế nào?
7. (L3) Bạn làm một yêu cầu rút idempotent thế nào để retry không double-spend?
8. (L3) Bạn xử lý webhook Sumsub trùng/sai thứ tự thế nào?
9. (L4) Một lần rút được submit hai lần (double-click / retry) — làm sao ngăn trả trùng?
10. (L4) Webhook Sumsub không bao giờ tới — làm sao tránh user kẹt ở "pending"?
11. (L4) Một mã TOTP bị chặn và replay — nó có dùng lại được không?
12. (L5) Ký rút custodial đồng bộ vs async — trade-off?
13. (L5) Lưu secret 2FA: mã hoá-at-rest vs secret manager ngoài — trade-off?
14. (L6) Tiền có vẻ đã rời đi nhưng user nói không rút — bạn điều tra thế nào?
15. (L6) Một provider KYC outage chặn onboarding — bạn degrade thanh lịch thế nào?
16. (L7) Phát hiện nạp qua nhiều chuỗi ở quy mô lớn — giữ đáng tin thế nào?
17. (L7) Xác minh 2FA dưới khối lượng login cao — có nút thắt không?
18. (L8) Bạn xây rút thành một saga/state machine qua các service không? Vì sao?
19. (L8) WebAuthn/passkey vs TOTP — bạn có migrate không? Trade-off?
20. (L8) Bạn thiết kế phê duyệt rút chống một tấn công insider/tài khoản bị chiếm thế nào?

### 6. Câu Trả Lời Chuẩn
1. "Danh tính và tiền: đăng ký và vòng đời user, xác minh KYC, nạp và rút qua các chuỗi, và 2FA gating các hành động nhạy cảm. Nó là lõi niềm tin của sàn."
2. "TOTP là một mã dùng-một-lần theo thời gian suy ra từ một secret chung và thời gian hiện tại — app authenticator và server tính cùng mã một cách độc lập. Bạn yêu cầu nó trên rút để dù mật khẩu bị đánh cắp cũng không thể di chuyển tiền mà không có yếu tố thứ hai."
3. "Webhook-driven và async. Bạn khởi động xác minh, rồi Sumsub gọi lại với kết quả, nên service mô hình hoá KYC thành một state machine (pending → approved/rejected) và coi webhook là source of truth, đối chiếu thay vì giả định một câu trả lời đồng bộ."
4. "Vì đây là dữ liệu tài khoản, danh tính, và cận-tiền cần tính nhất quán mạnh, ràng buộc, và transaction — đúng thế mạnh của Postgres. Bạn muốn các đảm bảo quan hệ và ràng buộc duy nhất ở đây, không phải eventual consistency."
5. "KYC: not-started → pending → approved/rejected (với re-submit). Rút: requested → 2FA-verified → approved → broadcast → confirmed/failed. Điểm của các state tường minh là bạn không thể bỏ qua một gate như 2FA hay xác minh."
6. "Secret TOTP là nhạy cảm — lưu mã hoá at-rest, lý tưởng qua một secret manager/KMS được quản lý thay vì plaintext trong một cột, và chỉ dùng phía server để validate mã. Nó không bao giờ lộ sau enrollment ngoài lần provisioning ban đầu."
7. "Mỗi yêu cầu rút mang một idempotency key do client cấp (hoặc một request id duy nhất), và DB bắt buộc tính duy nhất — một retry với cùng key trả về lần rút đã tồn tại thay vì tạo cái thứ hai. Nên double-submit gộp thành một."
8. "Webhook được key theo id sự kiện xác minh/applicant và xử lý idempotent — một callback trùng là no-op, và em áp các chuyển state một cách phòng thủ (không lùi) để một 'pending' sai thứ tự sau 'approved' không làm regress tài khoản."
9. "Idempotency key trên yêu cầu cộng một state machine — yêu cầu đầu tạo lần rút và chuyển nó khỏi 'requested'; một submit trùng thứ hai hoặc khớp idempotency key hoặc thấy state đã tiến, nên không thể tạo một payout thứ hai."
10. "Đừng chỉ dựa vào webhook — poll/đối chiếu status provider theo timer cho các bản ghi 'pending' bị kẹt để một callback bị mất tự lành. Webhook là đường nhanh; đối chiếu là lưới an toàn."
11. "Không — mã TOTP dùng-một-lần trong cửa sổ thời gian của nó; một khi chấp nhận em đánh dấu mã/cửa sổ đó đã dùng nên một replay trong cùng cửa sổ bị reject, và cửa sổ hết hạn lo phần còn lại. Dùng-một-lần + cửa sổ ngắn đánh bại replay."
12. "Ký đồng bộ block request ở bước custody/broadcast và ghép availability; async (đưa lần rút vào queue, ký/broadcast trong một worker, xác nhận qua callback) tách nó và cho retry an toàn — đổi lại luồng là eventually-completed, cái đó ổn vì người dùng kỳ vọng rút mất thời gian."
13. "Mã hoá at-rest trong DB đơn giản hơn nhưng quản khoá là việc của bạn; một secret manager/KMS ngoài tập trung xử lý khoá và xoay khoá, đổi lại một dependency. Với secret 2FA em thiên về xử lý qua KMS vì bán kính nổ của một rò rỉ là yếu tố thứ hai của mọi user."
14. "Trace bản ghi rút: có một xác minh 2FA hợp lệ không, một idempotency key khớp, các chuyển state và ai/cái gì kích hoạt chúng, và tx on-chain. Audit trail đó cho bạn biết đó là một hành động đã xác thực hợp lệ, một bug, hay tài khoản bị chiếm."
15. "Degrade về một state 'xác minh bị trễ' rõ ràng và đưa applicant vào queue thay vì hard-fail đăng ký, để không mất user — và gate các hành động liên quan tiền cho tới khi KYC thực sự clear. Bạn giữ onboarding chạy mà không để ai bỏ qua xác minh."
16. "Watcher nạp per-chuỗi với ngưỡng confirmation, credit idempotent key theo tx hash, và đối chiếu với state chuỗi — nên một phát hiện bị lỡ hoặc trùng tự sửa và một lần nạp được credit đúng một lần bất kể thấy bao nhiêu lần."
17. "Validate TOTP là compute rẻ (vài HMAC qua các cửa sổ kề), nên nó không phải nút thắt thật; thứ cần theo dõi là store check mã-đã-dùng dưới khối lượng cao, một phép ghi key nhỏ. Nó scale ổn với caching/DB tuning bình thường."
18. "Có — một lần rút là một saga tự nhiên: reserve tiền → verify 2FA → approve → broadcast → confirm, mỗi bước compensatable. Mô hình hoá nó thành một state machine/saga tường minh làm các hỏng một-phần hồi phục được và audit được thay vì để tiền lơ lửng."
19. "Passkey/WebAuthn chống phishing theo cách TOTP không (mã TOTP có thể bị phish real-time), nên dài hạn migrate là một thắng lợi bảo mật. Trade-off là phức tạp UX thiết bị/recovery; em sẽ cung cấp WebAuthn và giữ TOTP làm fallback thay vì cutover cứng."
20. "Defense in depth: 2FA cộng allow-list địa chỉ rút với một độ trễ thời gian trên địa chỉ mới, thông báo mỗi lần rút, và giới hạn velocity/anomaly — nên một yếu tố bị chiếm đơn lẻ hay một insider không thể âm thầm rút cạn tiền mà không kích một gate hay một alert."

### 7. Whiteboard Version
Vẽ **service tài khoản** với **Postgres** dưới nó, rồi ba luồng thành các state machine nhỏ. **KYC:** `start →
pending → approved/rejected`, với **Sumsub** ở một bên gửi một **webhook** về (+ một mũi tên poller đối chiếu).
**Rút:** `requested → 2FA verified → approved → broadcast → confirmed`, với **idempotency key** khoanh tròn trên
bước "requested" và **TOTP** gating bước "2FA verified". **Nạp:** chain watcher → credit idempotent key theo tx hash.
Người phỏng vấn ngắt ở "làm sao chặn double withdrawal" (idempotency key + state machine) và "webhook không bao giờ
tới" (poller đối chiếu). Nói rằng bạn *contributed* vào service này để scope rõ ràng.

### 8. Lỗi Thường Gặp
- **"Em check 2FA trong handler rồi xử lý."** Nghe như gating tuỳ tiện. Tốt hơn: "2FA là một chuyển state bắt buộc; bạn không thể tới 'approved' mà không có nó."
- **"Em tin response Sumsub."** Mong manh. Tốt hơn: "Webhook là source of truth, xử lý idempotent, với một poller đối chiếu cho callback bị mất."
- **Nói quá scope.** Bullet ghi "contributed". Tốt hơn: "em đóng góp vào service tài khoản" — đừng nói bạn xây cả thứ đó.

### 9. Red Flags
- "TOTP 2FA" — sẵn sàng cho độ sâu bảo mật thật (lưu secret, replay, phishing); trả lời nông ở đây gây hại vì đó là bảo mật. Biết dùng-một-lần + secret-at-rest.
- "nạp/rút đa chuỗi" — làm rõ scope; nếu bạn không xây tầng custody/ký, nói bạn làm luồng phía tài khoản, không phải hạ tầng HSM/ký.
- "Sumsub KYC" — biết nó async/webhook-based; mô tả nó như một API call đơn giản báo hiệu bạn không thật sự tích hợp nó.

### 10. Kịch Bản Chốt
"Em cũng đóng góp vào service tài khoản — lõi danh tính và tiền của sàn: vòng đời user, KYC qua Sumsub, nạp và rút đa
chuỗi, và TOTP 2FA, tất cả trên Postgres. Đó là code chung, rủi ro cao, nên em làm với team. Các phần em thấy thú vị
nhất là các mối quan tâm về state-machine và idempotency. KYC là async và webhook-driven, nên em mô hình hoá nó thành
pending/approved/rejected với webhook là source of truth cộng một poller đối chiếu cho callback bị mất. Rút được gate
bởi một state machine tường minh — bạn không thể tới 'approved' mà không qua 2FA — và mỗi yêu cầu mang một idempotency
key được DB bắt buộc để một double-submit không thể double-spend. Mã TOTP dùng-một-lần trong cửa sổ của nó để chặn
replay, và secret lưu mã hoá thay vì plaintext. Làm việc trên nó thực sự làm em thấm rằng nhiều phần của 'bảo mật'
chỉ là state machine cẩn thận và idempotency — giữ hệ thống ra khỏi các trạng thái xấu. Em sẵn lòng đi sâu hơn vào
saga rút hay cách em thêm allow-list địa chỉ."

---

# PHẦN 3 — Kotoba Press (nền tảng học tiếng Nhật, cá nhân)

> Mẹo khung: đây là một dự án *cá nhân*, nên nó là tín hiệu "em làm vì đam mê / em quan tâm tới craft". Nhấn vào
> các lựa chọn có chủ đích (hexagonal, tự viết search engine bằng C++ thay vì dùng Elasticsearch) và thành thật về
> các cảnh báo benchmark — chính sự thành thật đó là một tín hiệu senior mạnh.

---

## Bullet K1 — Backend Go hexagonal (ports-and-adapters, domain error, slog, OAuth2+JWT, composition-root DI)

> *"Architected a hexagonal Go backend (Gin, MongoDB) with ports-and-adapters layering, domain error handling,
> `slog` logging, Google OAuth2 + JWT, and composition-root DI across API, importer, and indexer binaries."*

### 1. Ý đồ (Intent)
Đây là bullet **kiến trúc phần mềm / thiết kế sạch**. Nó cho thấy dù làm một mình, không ai ép, bạn vẫn xây các hệ
thống maintainable, testable — bạn hiểu *vì sao* layering và DI quan trọng, không chỉ *thế nào*. Ấn tượng: *người
này nghĩ về maintainability dài hạn và testability theo mặc định, và trình bày được kiến trúc.*

### 2. Elevator Pitch (20–30 giây)
"Dự án cá nhân chính của em là Kotoba Press, một app học tiếng Nhật, và em xây backend bằng Go dùng kiến trúc
hexagonal — ports và adapters. Nên logic miền nằm ở giữa mà không biết nó đang nói chuyện với Mongo hay gRPC hay
HTTP; tất cả nằm sau các interface. Em ghép nó lại bằng composition-root DI qua ba binary — API, một importer, và một
indexer — dùng chung cùng một lõi. Em cũng làm domain error handling đàng hoàng, structured logging với slog, và
Google OAuth2 với JWT cho auth."

### 3. Bản Sâu (1–2 phút)
"Kotoba Press là một dự án cá nhân, nên em dùng nó một phần như một nơi để xây thứ theo cách em nghĩ *nên* xây khi
không có áp lực deadline.

Kiến trúc là hexagonal — ports và adapters. Ý tưởng là logic miền nằm ở tâm và định nghĩa các interface (ports) cho
mọi thứ nó cần — một repository, một search client, bất cứ gì — và các implementation cụ thể (adapters) nằm ở rìa.
Nên logic nghiệp vụ lõi thật sự không import Mongo hay Gin hay gRPC; nó phụ thuộc vào interface, và các tầng ngoài
cắm vào. Cái được là testability và swappability — em test được miền với fake, và khi em đổi implementation search
thì lõi không thay đổi.

Em có ba binary — API server, một importer nạp dữ liệu từ điển, và một indexer feed search engine — và chúng dùng
chung cùng một lõi miền. Đó là chỗ composition-root DI vào cuộc: mỗi binary có một chỗ duy nhất (composition root,
trong `main`) nơi em construct các adapter cụ thể và inject chúng. Không có DI container ma thuật — nó là wiring
tường minh, mà trong Go em thật sự thích hơn vì bạn đọc được chính xác cái gì nối với cái gì.

Với các mối quan tâm phụ trợ: domain error handling nghĩa là error được mô hình hoá thành khái niệm miền (not-found,
validation, v.v.) và map sang response ở tầng transport tại rìa, nên lõi không biết về HTTP status code. Logging là
`slog`, structured, nên log query được thay vì string lộn xộn. Auth là Google OAuth2 để login cộng JWT cho validate
session stateless trên các request.

Kết quả: một backend nơi logic thú vị được cô lập, testable, và dùng chung sạch qua ba binary. Bài học — củng cố từ
RaidenX — là các đường ranh giới chính là kiến trúc; một khi ports được định nghĩa tốt, mọi thứ còn lại chỉ là
adapter, và thay đổi vẫn rẻ."

### 4. Interview Hooks
- "hexagonal … ports và adapters" → *Vì sao hexagonal cho một dự án cá nhân? Ports của bạn là gì?*
- "composition-root DI qua binary API, importer, indexer" → *Vì sao ba binary? Vì sao không DI framework?*
- "domain error handling" → *Domain error map sang HTTP thế nào? Error wrapping?*
- "Google OAuth2 + JWT" → *Vì sao JWT? Xử lý hết hạn/thu hồi thế nào?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Kiến trúc hexagonal theo lời của bạn là gì?
2. (L1) Vì sao bận tâm với nó trên một dự án cá nhân?
3. (L2) Các ports thực tế trong app của bạn là gì, và adapter của chúng?
4. (L2) Vì sao chia thành ba binary thay vì một?
5. (L2) Composition root là gì và vì sao không DI container?
6. (L3) Domain error map sang HTTP response thế nào mà lõi không biết HTTP?
7. (L3) Bạn cấu trúc middleware validate JWT thế nào và trong token có gì?
8. (L3) Luồng OAuth2 hoạt động từ đầu tới cuối thế nào ở đây?
9. (L4) Một adapter downstream (Mongo/gRPC) hỏng — miền biểu lộ nó thế nào?
10. (L4) Một JWT bị đánh cắp — bạn giới hạn thiệt hại thế nào?
11. (L5) Hexagonal thêm indirection/boilerplate — khi nào nó không đáng?
12. (L5) JWT vs session phía server — vì sao bạn chọn JWT ở đây?
13. (L6) Bạn cần đổi MongoDB sang Postgres — bao nhiêu thứ thay đổi?
14. (L6) Một bug chỉ hiện ở binary indexer — thiết kế lõi-dùng-chung giúp debug thế nào?
15. (L7) Nếu nó lớn lên thành một team 10 người, kiến trúc này giúp hay hại onboarding?
16. (L7) Ba binary cần scale độc lập — thiết kế có hỗ trợ không?
17. (L8) Bạn có dùng `google/wire` hay fx cho DI ở quy mô lớn hơn không? Trade-off vs thủ công?
18. (L8) Hexagonal vs clean architecture vs chỉ packages-by-feature — vì sao hexagonal?
19. (L8) Bạn thêm một transport thứ hai (gRPC API) song song với Gin HTTP API thế nào?
20. (L8) Bạn test miền cô lập thế nào — fake cái gì và không fake cái gì?

### 6. Câu Trả Lời Chuẩn
1. "Logic miền nằm ở tâm và định nghĩa interface cho cái nó cần; công nghệ cụ thể (DB, web framework, RPC) nằm ở rìa và cắm vào các interface đó. Dependency trỏ vào trong — lõi không bao giờ phụ thuộc ra ngoài."
2. "Vì nó làm lõi testable và cho em đổi implementation mà không đụng logic nghiệp vụ — thứ em thật sự đã làm khi đổi search backend. Kể cả một mình, cái đó trả cổ tức ngay lần đầu bạn đổi ý về một dependency."
3. "Các ports như một vocabulary repository, một search client, một auth provider — mỗi cái là một interface miền sở hữu. Adapter là Mongo repo, gRPC search client, implementation OAuth2/JWT. Miền chỉ thấy các interface."
4. "Chúng có vòng đời khác nhau: API phục vụ request, importer là một batch job nạp dữ liệu, indexer feed search engine. Binary riêng cho phép mỗi cái chạy và scale theo lịch riêng trong khi dùng chung cùng lõi miền."
5. "Composition root là chỗ duy nhất — trong main của mỗi binary — nơi em construct các adapter cụ thể và inject chúng vào miền. Không container vì wiring tường minh trong Go dễ đọc và được compile check; một container thêm ma thuật runtime mà em không cần ở kích thước này."
6. "Miền trả các domain error có kiểu — not-found, validation, conflict. Một tầng dịch ở rìa HTTP map chúng sang status code. Nên lõi diễn đạt *cái gì* sai về mặt ngữ nghĩa, và chỉ adapter biết not-found nghĩa là 404."
7. "Middleware validate chữ ký và hết hạn của JWT, trích identity/claims, và đặt vào context của request; handler đọc identity từ context, không bao giờ tự parse token. Token mang identity và hết hạn, giữ tối thiểu."
8. "Redirect sang Google, user consent, Google trả một code, em đổi nó phía server lấy identity của user, rồi phát JWT của riêng em cho các request sau. Nên Google lo authentication; JWT của em lo state session sau đó."
9. "Adapter trả một error, miền wrap nó thành một domain-level error (ví dụ dependency-failure hay not-found), và rìa map cái đó sang response đúng. Lõi phản ứng với *ngữ nghĩa* hỏng, không phải chi tiết Mongo/gRPC."
10. "JWT là stateless nên JWT thuần không thu hồi tức thì được — em giữ hết hạn ngắn để giới hạn cửa sổ, và để thu hồi thật bạn thêm một check phía server (denylist/vô hiệu refresh-token). Access ngắn hạn + refresh là mitigation thực tế."
11. "Khi app thật sự là một tầng CRUD mỏng không có logic miền thật, indirection ports/adapters là overhead không lợi ích. Hexagonal trả cổ tức khi có logic miền đáng kể cần bảo vệ và các dependency bạn có thể đổi — nếu không thì là nghi thức."
12. "JWT cho validate stateless — không lookup session store mỗi request, hợp một API stateless nhỏ. Cái giá là thu hồi khó hơn, cái em chấp nhận cho một app cá nhân bằng hết hạn ngắn. Cho một ngân hàng em sẽ cân nhắc lại về phía session phía server."
13. "Lý tưởng chỉ là repository adapter — implement cùng repository port với Postgres và cắm nó vào composition root. Miền và các tầng khác không thay đổi. Đó là cả điểm của port; em tiến gần 'chỉ adapter' tới đâu là bài test xem ranh giới của em có thành thật không."
14. "Vì lõi dùng chung, em có thể tái hiện hành vi miền trong một test với fake và xác nhận bug nằm trong adapter/wiring của indexer, không phải logic dùng chung. Lõi dùng chung thu hẹp 'nó ở đâu' nhanh — nếu API chạy mà indexer không, thì là adapter."
15. "Giúp — người mới học miền ở tâm mà không cần hiểu mọi rìa, và composition root tường minh là một bản đồ dễ đọc về cách mọi thứ nối. Rủi ro là indirection làm rối người mới với pattern, nên cần một README ngắn."
16. "Có — binary riêng deploy và scale độc lập, nên API chạy nhiều replica trong khi importer là một job thỉnh thoảng. Dùng chung miền như một library không ghép runtime scaling của chúng."
17. "Ở quy mô lớn hơn `wire` cho DI compile-time mà không phải tự viết hết wiring, giúp khi graph lớn. Em chọn thủ công vì graph nhỏ và wiring tường minh dễ đọc nhất; em sẽ dùng wire khi boilerplate constructor vượt sự rõ ràng của nó, không phải trước đó."
18. "Chúng là anh em họ — tất cả đẩy dependency vào trong. Em thích cách hexagonal khung 'ports miền sở hữu, adapter ở rìa' vì nó map sạch lên 'em có thể đổi dependency này'. Packages-by-feature cũng ổn, nhưng hexagonal làm swappability tường minh, đó là mục tiêu."
19. "Thêm một gRPC adapter như một inbound transport khác gọi cùng các domain service — lõi không thay đổi vì nó không biết về transport. Cả HTTP và gRPC thành các rìa mỏng trên cùng các use case. Đó chính xác là thứ ports làm rẻ."
20. "Em fake các outbound port — repository, search client — bằng implementation in-memory và test các use case miền trực tiếp, không DB không network. Em không fake bản thân miền; điểm là để chạy logic nghiệp vụ thật với các rìa fake."

### 7. Whiteboard Version
Vẽ **hình lục giác** kinh điển: miền/lõi ở tâm, **ports là interface trên ranh giới**, **adapter ở ngoài** (Mongo
repo, gRPC search client, Gin HTTP handler, OAuth2/JWT). Vẽ **mũi tên dependency trỏ vào trong** và nói câu đó một
lần — đó là cả ý tưởng. Rồi ở một bên vẽ **ba binary** (API, importer, indexer) mỗi cái với một **hộp composition-root**
nhỏ wiring adapter vào lõi dùng chung. Người phỏng vấn ngắt ở "ports thực tế của bạn là gì" (gọi tên cụ thể) và "vì
sao không DI framework" (wiring tường minh, được compile check). Nếu được hỏi, cho thấy đổi Mongo→Postgres như thay
một adapter.

### 8. Lỗi Thường Gặp
- **"Em dùng hexagonal vì nó là best practice."** Cargo-cult. Tốt hơn: gọi tên cái được cụ thể (đổi search backend mà không đụng lõi).
- **"DI framework lo wiring."** Với Go cái này có thể nghe như over-engineering. Tốt hơn: "composition root tường minh — dễ đọc và được compile check."
- **"JWT nên nó bảo mật."** Mơ hồ. Tốt hơn: nói về validate stateless và trade-off thu hồi một cách thành thật.

### 9. Red Flags
- "architected" trên một dự án cá nhân — được, nhưng giữ nó có căn cứ; ghép với các quyết định cụ thể để không nghe phóng đại.
- Đảm bảo bạn gọi tên được ports/adapter thật; nếu chỉ nói được bằng trừu tượng, nó nghe như bạn đọc về hexagonal chứ không ship nó.
- Thu hồi JWT là điểm yếu đã biết — thừa nhận chủ động thay vì nói JWT hoàn toàn tốt hơn session.

### 10. Kịch Bản Chốt
"Dự án cá nhân chính của em là Kotoba Press, một app học tiếng Nhật, và em xây backend Go theo hexagonal vì em muốn
logic miền được bảo vệ khỏi hạ tầng. Lõi định nghĩa ports — một vocabulary repository, một search client, một auth
provider — và các implementation Mongo, gRPC, OAuth2 cụ thể là adapter ở rìa, nên lõi không bao giờ import cái nào.
Cái đó trả cổ tức cụ thể khi em đổi search backend mà miền không nhúc nhích. Có ba binary — API, importer, indexer —
dùng chung lõi đó, wiring bằng một composition root tường minh trong mỗi main thay vì một DI framework, vì wiring
tường minh trong Go dễ đọc và được compile check. Error được mô hình hoá thành domain error và map sang HTTP ở rìa,
logging structured với slog, và auth là Google OAuth2 cộng JWT. Nếu hữu ích em có thể đi qua cách em đổi Mongo sang
Postgres — lý tưởng chỉ là một adapter — hay cách em test miền cô lập với fake."

---

## Bullet K2 — Search engine BM25 C++17 tự viết qua gRPC/protobuf (client-streaming indexing, gRPC-first với fallback Mongo)

> *"Integrated a custom C++17 BM25 search engine over gRPC/protobuf (client-streaming bulk indexing, gRPC-first
> search with MongoDB regex fallback), sustaining ~15K docs/sec indexing and ~0.04 ms median query latency."*

### 1. Ý đồ (Intent)
Đây là **món trưng bày systems-programming + đa ngôn ngữ + kỹ thuật có chủ đích**. Tự viết engine BM25 bằng C++ thay
vì với tay lấy Elasticsearch là một tín hiệu mạnh "em hiểu nền tảng và xây được qua ranh giới ngôn ngữ" — miễn là bạn
khung nó thành thật (bạn làm một phần vì mục tiêu học). Ấn tượng: *người này hiểu nội tại search, gRPC/protobuf, thiết
kế đa ngôn ngữ, và ra các quyết định build-vs-buy có ý thức.*

### 2. Elevator Pitch (20–30 giây)
"Phần em tự hào nhất ở dự án đó là em tự viết một search engine bằng C++ thay vì dùng Elasticsearch. Nó implement
ranking BM25, và backend Go nói chuyện với nó qua gRPC với protobuf. Indexing dùng client-streaming để em bulk-load
document hiệu quả, và search là gRPC-first với một fallback MongoDB regex nếu engine không sẵn sàng. Trên benchmark
của em nó đạt khoảng 15K doc mỗi giây indexing và tầm 0.04 ms median query latency — dù em cẩn thận với mấy con số đó,
chúng là benchmark tổng hợp chạy một lần."

### 3. Bản Sâu (1–2 phút)
"Điểm xuất phát thành thật: em có thể dùng Elasticsearch, nhưng em cố ý tự viết search engine bằng C++ vì hai lý do —
một, để giữ app gọn nhẹ về vận hành thay vì chạy cả một cluster Elasticsearch cho một dự án cá nhân, và hai, thật lòng
như một mục tiêu systems-programming, vì em muốn thật sự hiểu nội tại search chứ không coi nó là hộp đen.

Engine implement BM25, hàm ranking chuẩn — nó chấm điểm document theo term frequency và inverse document frequency với
length normalization, nên nó là một mô hình relevance thật, không chỉ khớp chuỗi con. Nó là một process C++17 riêng,
và backend Go giao tiếp với nó qua gRPC dùng protobuf, nên em có một contract có kiểu, hiệu quả qua ranh giới ngôn ngữ.

Hai lựa chọn thiết kế em thích ở đó. Indexing dùng gRPC client-streaming — thay vì một RPC mỗi document, client stream
cả một batch document trong một call, hiệu quả hơn nhiều cho bulk load và là chỗ ~15K doc/giây đến từ. Và search là
gRPC-first với một fallback MongoDB regex — nên đường chính là engine BM25 nhanh, nhưng nếu nó down hoặc không sẵn
sàng, app degrade về một search Mongo regex thay vì fail search hoàn toàn. Đó là một quyết định resilience: search vẫn
sống, chỉ chậm hơn và kém relevant hơn, trong lúc engine outage.

Về các con số — ~15K doc/giây indexing, ~0.04 ms median query — em muốn nói thẳng rằng chúng là benchmark tổng hợp
chạy một lần trên một corpus cố định, không có confidence interval, và em thực ra thấy một kết quả kỳ lạ là hai thread
chậm hơn một, cái đó chỉ vào contention em chưa đào tới cùng. Nên em tin ở mức độ lớn, không phải con số chính xác.

Kết quả: một đường search tự chứa, nhanh với một fallback thanh lịch, và một hiểu biết thật về BM25 và gRPC streaming.
Bài học nhiều về benchmark thành thật cũng như về search — dễ trích một con số; khó hơn là biết nó thật sự nghĩa là gì."

### 4. Interview Hooks
- "search engine BM25 C++17 tự viết" → *Vì sao build vs Elasticsearch? BM25 là gì? Vì sao C++?*
- "client-streaming bulk indexing" → *Vì sao streaming? Inverted index được xây thế nào?*
- "gRPC-first search với fallback MongoDB regex" → *Vì sao một fallback? Bạn quyết định fallback thế nào?*
- "~15K doc/giây … ~0.04 ms" → *Bạn đo thế nào? Nút thắt là gì? (Bạn có thể chủ động nói các cảnh báo.)*

### 5. Câu Hỏi Đào Sâu
1. (L1) BM25 là gì và vì sao dùng nó thay vì khớp chuỗi con/regex đơn giản?
2. (L1) Vì sao tự viết engine thay vì Elasticsearch?
3. (L2) Engine cấu trúc thế nào — inverted index, postings, scoring?
4. (L2) Vì sao gRPC/protobuf qua ranh giới Go↔C++ thay vì REST hay FFI?
5. (L2) Vì sao client-streaming cho indexing cụ thể?
6. (L3) Bạn xây và lưu inverted index thế nào — trong bộ nhớ, trên đĩa?
7. (L3) Một query được scored từ đầu tới cuối thế nào?
8. (L3) Logic gRPC-first-với-fallback quyết định fallback thế nào?
9. (L4) Engine C++ crash giữa chừng index-stream — bạn còn lại ở state nào?
10. (L4) Engine và Mongo bất đồng kết quả lúc fallback — có phải vấn đề không?
11. (L5) Build-vs-buy: tự viết tốn bạn cái gì so với Elasticsearch?
12. (L5) Index in-memory (nhanh, dễ bay) vs persistent — bạn chọn cái nào và vì sao?
13. (L6) Bạn đo 2 thread chậm hơn 1 — giả thuyết của bạn là gì và bạn xác nhận thế nào?
14. (L6) Peak RSS ~211MB — chuyện gì xảy ra khi corpus lớn 100x?
15. (L7) Bạn scale nó tới hàng triệu document vượt một process thế nào?
16. (L7) Bạn xử lý cập nhật/xoá index, không chỉ bulk load, thế nào?
17. (L8) Bạn thêm phrase/fuzzy/prefix query mà hiện chưa hỗ trợ thế nào?
18. (L8) Bạn có shard index không? Query fan-out/merge hoạt động thế nào?
19. (L8) Thành thật, benchmark của bạn đáng tin cỡ nào, và cái gì làm nó đáng tin?
20. (L8) Nếu cái này lên production thật, bạn giữ nó hay chuyển sang Elasticsearch/Tantivy? Vì sao?

### 6. Câu Trả Lời Chuẩn
1. "BM25 rank document theo relevance — term frequency, inverse document frequency, và length normalization — nên từ phổ biến ít quan trọng hơn và từ khớp hiếm quan trọng hơn. Regex/chuỗi con chỉ nói *có* một từ xuất hiện không, không phải document *relevant* cỡ nào; BM25 cho bạn kết quả có rank."
2. "Hai lý do thành thật: giữ một dự án cá nhân gọn nhẹ về vận hành thay vì chạy một cluster Elasticsearch, và như một bài tập systems-programming có chủ đích để thật sự hiểu nội tại search. Không phải 'Elasticsearch dở' — mà là một quyết định build-để-học-và-giữ-gọn có ý thức."
3. "Một inverted index — term → postings list các document (và term frequency) — cộng metadata document cho length normalization. Một query lookup postings mỗi term và cộng dồn điểm BM25 qua các document khớp, rồi trả top-k."
4. "gRPC/protobuf cho một contract có kiểu, được version, hiệu quả qua một ranh giới process và qua hai ngôn ngữ, với streaming built-in. FFI sẽ ghép chặt các binary và chia sẻ một không gian bộ nhớ em không muốn; REST sẽ chatty hơn và không kiểu. gRPC hợp một service riêng throughput cao nhất."
5. "Vì indexing vốn là bulk — một RPC mỗi document nghĩa là overhead per-call lấn át. Client-streaming cho client đẩy cả một batch qua một call, nên engine ingest liên tục; sự phân bổ đều đó là thứ đưa throughput lên tầm ~15K/giây."
6. "Chủ yếu một inverted index in-memory cho tốc độ query, xây khi document stream vào. Đó là vì sao query latency tí xíu — nó là lookup bộ nhớ và số học — và cũng là vì sao bộ nhớ/persistence là mối lo scale chính, cái em ý thức được."
7. "Tokenize query, lookup postings mỗi term, và với mỗi candidate document cộng dồn đóng góp BM25 của mọi query term nó khớp, rồi giữ một top-k heap. Nên nó là postings intersection/union cộng scoring cộng một top-k selection."
8. "Client thử gRPC search; khi error hoặc không sẵn sàng nó fallback về một MongoDB regex query. Đó là một fallback health/error-driven — đường chính là engine nhanh, và fallback đổi relevance và tốc độ lấy availability để search không bao giờ hard-fail."
9. "Vì nó là một index in-memory xây từ một stream, một crash giữa stream mất index đang dở — đó là vì sao indexing là một job bulk chạy lại được, không phải source of truth. Dữ liệu nguồn nằm trong Mongo, nên em chỉ re-index; engine là một view suy ra, xây lại được."
10. "Nó được kỳ vọng — fallback là khớp chuỗi con regex, nên kém relevant và không rank so với BM25. Đó là một degradation chấp nhận được lúc outage; điểm là 'search vẫn chạy', không phải 'kết quả giống hệt'. Em sẽ không dựa vào chúng khớp nhau."
11. "Nó tốn em các tính năng Elasticsearch cho miễn phí — fuzzy, phrase, prefix, analyzer, phân tán, persistence — và gánh nặng tự lo correctness/benchmark. Em mua sự đơn giản vận hành và hiểu biết sâu. Cho một sản phẩm thật với các nhu cầu đó, trade đó lật về phía buy."
12. "In-memory cho tốc độ và đơn giản, chấp nhận dễ bay vì index xây lại được từ Mongo. Trade-off là bộ nhớ giới hạn kích thước corpus và một restart nghĩa là re-index. Cho quy mô lớn hơn em sẽ thêm persistence hoặc segment memory-mapped."
13. "Giả thuyết của em là lock contention hoặc oversubscription trong đường indexing đa thread — cách shard-merge có lẽ có overhead đồng bộ lấn át ở 2 thread. Em sẽ xác nhận bằng một lock/CPU profiler, cái em rõ ràng không chạy — nên bây giờ nó là một giả thuyết, không phải một chẩn đoán."
14. "Ở 100x index in-memory sẽ không vừa thoải mái, nên RSS thành bức tường. Em sẽ chuyển sang lưu trữ segment/memory-mapped hoặc shard qua các process. Thiết kế in-memory một-process hiện tại đúng cho kích thước corpus em test, không phải cho 100x."
15. "Shard index qua các process/node theo document, fan một query ra mọi shard, và merge top-k từ mỗi cái. Đó về cơ bản là cái Elasticsearch làm — tại điểm đó em sẽ nghiêm túc cân nhắc dùng một engine có sẵn thay vì tự implement lại phân tán."
16. "Xoá thường là tombstone trong postings với compaction định kỳ; cập nhật là xoá-rồi-reindex document. Đường hiện tại của em thiên về bulk-load, nên cập nhật/xoá incremental đúng là loại tính năng nơi độ chín của một engine trưởng thành bắt đầu thắng."
17. "Chúng là bổ sung trên inverted index — prefix qua một term dictionary/trie, phrase qua positional postings, fuzzy qua edit-distance trên term dictionary. Em không implement chúng (em liệt chúng là excluded), đó là scope thành thật; mỗi cái là một khối việc thật."
18. "Shard theo document, query mọi shard song song, merge theo điểm cho top-k toàn cục — length normalization cần cẩn thận để điểm per-shard so sánh được (hoặc bạn gom stat toàn cục). Fan-out/merge là cách chuẩn; đúng global IDF qua các shard là phần tinh tế."
19. "Thành thật, không mấy — một run mỗi config, corpus tổng hợp, judgment relevance tổng hợp, không profiler, và một dị thường contention em chưa đào. Em tin ở mức độ lớn. Để làm chúng đáng tin em sẽ chạy lặp với confidence interval, một corpus thực tế, relevance gán bởi người, và profiling thật."
20. "Cho một sản phẩm thật em có lẽ sẽ chuyển sang thứ như Elasticsearch hay một library Rust như Tantivy — em sẽ có phân tán, query phong phú hơn, và persistence mà không phải bảo trì một engine. Em chỉ giữ cái của em nếu sự gọn nhẹ vận hành thật sự vượt các cái đó, mà với đa số sản phẩm thì không. Xây nó đáng vì hiểu biết, không nhất thiết cho production."

### 7. Whiteboard Version
Vẽ **Go API** và **search engine C++** như hai process riêng với một ống **gRPC/protobuf** giữa chúng — ghi nhãn ống,
nó là ranh giới then chốt. Vẽ **hai RPC**: `IndexDocuments (client-streaming)` với nhiều doc chảy vào một call, và
`Search` trả kết quả có rank. Bên trong hộp C++, vẽ **inverted index** (term → postings) và ghi chú "in-memory, xây
lại được từ Mongo." Từ phía Go, vẽ **fallback**: `Search → gRPC (chính)`, khi hỏng `→ Mongo regex`. Người phỏng vấn
ngắt ở "vì sao không Elasticsearch" (build-để-học + gọn) và "làm sao tin mấy con số đó" (chủ động nói các cảnh báo —
nó rơi vào như sự trưởng thành). Vẽ Mongo vừa là fallback vừa là source of truth để re-index.

### 8. Lỗi Thường Gặp
- **"Engine của em nhanh hơn Elasticsearch."** Nói quá không bảo vệ được. Tốt hơn: "Nó nhanh cho corpus của em, nhưng Elasticsearch cho các tính năng và phân tán em không xây."
- **Trích 15K/0.04ms như sự thật cứng.** Một người phỏng vấn sắc sảo sẽ dò methodology. Tốt hơn: chủ động nói chúng là benchmark tổng hợp chạy một lần — sự thành thật nghe như senior.
- **"Em xây nó vì Elasticsearch cồng kềnh."** Giáo điều. Tốt hơn: "gọn nhẹ vận hành cho một dự án cá nhân, cộng em muốn học nội tại search."

### 9. Red Flags
- Các con số hiệu năng là red flag lớn nhất — luôn kèm các cảnh báo (một run, tổng hợp, không profiler, dị thường 2-thread-chậm-hơn). Cách khung dễ bảo vệ: *"mức độ lớn, không phải chính xác."*
- "engine BM25 tự viết" — sẵn sàng thật sự giải thích BM25 và inverted index; nói nó mà không có nội tại thì tệ hơn không nói.
- Đừng ngụ ý nó production-grade — nói nó là một dự án cá nhân bạn có lẽ sẽ thay bằng Elasticsearch/Tantivy trong production.

### 10. Kịch Bản Chốt
"Phần em tự hào nhất ở Kotoba Press là em tự viết một search engine bằng C++ thay vì dùng Elasticsearch — một phần để
giữ một dự án cá nhân gọn nhẹ về vận hành, và thành thật một phần vì em muốn hiểu nội tại search tận tay. Nó implement
BM25 trên một inverted index in-memory, và backend Go của em nói chuyện với nó qua gRPC với protobuf. Hai lựa chọn em
thích: indexing là gRPC client-streaming, nên em đẩy cả một batch doc trong một call thay vì một RPC mỗi document —
đó là chỗ throughput đến từ — và search là gRPC-first với một fallback MongoDB regex, nên nếu engine down, search
degrade về kết quả chậm hơn, kém relevant hơn thay vì fail. Em có benchmark nó — tầm 15K doc mỗi giây và median query
dưới mili-giây — nhưng em cẩn thận với chúng: chúng là benchmark tổng hợp chạy một lần, không profiler, và em thậm chí
thấy hai thread ra chậm hơn một, cái em flag là contention chưa giải thích được. Nên em tin ở mức độ lớn, không phải
con số chính xác. Trong production em có lẽ sẽ chuyển sang Elasticsearch hay Tantivy cho tính năng và phân tán; xây cái
này là vì hiểu biết. Em sẵn lòng đi vào scoring BM25 hay cách em shard nó."

---

## Bullet K3 — Load-test API Go với k6 (19K req/s health, 4.4K req/s vocabulary, p99 < 32ms, ổn định tới 800 user)

> *"Load-tested the Go API with k6: 19K req/s on health and 4.4K req/s on vocabulary queries (p99 < 32 ms),
> stable to 800 concurrent users."*

### 1. Ý đồ (Intent)
Đây là bullet **performance-engineering + kỷ luật đo đạc**. Nó cho thấy bạn không chỉ xây mà còn *đo* — bạn dựng load
test, đọc latency percentile, và tìm điểm bão hoà. Ấn tượng: *người này validate hiệu năng bằng thực nghiệm và hiểu
percentile và bão hoà, không chỉ trung bình.*

### 2. Elevator Pitch (20–30 giây)
"Em load-test API với k6 để thật sự biết giới hạn của nó thay vì đoán. Em cô lập endpoint để tìm capacity — endpoint
health đạt khoảng 19K request mỗi giây, và vocabulary query, vốn đụng DB, đạt tầm 4.4K mỗi giây với p99 dưới 32
mili-giây — rồi em chạy một stress test workflow thực tế ramp concurrency, và nó ổn định tới 800 concurrent user.
Điểm là tìm chỗ nó bão hoà và cái gì hỏng trước, hoá ra là các endpoint DB-bound, đúng như dự đoán."

### 3. Bản Sâu (1–2 phút)
"Em muốn con số thật cho API, không phải cảm tính, nên em dựng k6 với hai loại test.

Thứ nhất, test capacity per-endpoint — cô lập một endpoint, dập nó với một số virtual user cố định, không pacing, và
xem throughput tối đa và latency percentile. Endpoint health đạt ~19K req/s, về cơ bản là trần framework/runtime vì
nó không làm việc thật — đó là baseline của em cho 'Go+Gin chạy nhanh cỡ nào khi không có gì cản.' Endpoint vocabulary,
vốn thật sự query MongoDB, đạt ~4.4K req/s với p99 dưới 32 ms. Khoảng cách giữa 19K và 4.4K là phần thú vị — nó định
lượng chi phí của round-trip DB và việc thật.

Thứ hai, một stress test workflow thực tế — mô phỏng hành vi user thật (login, review, learn, search) và ramp
concurrency: 10, 50, 100, tới 800 virtual user, trong khi ghi CPU và bộ nhớ trên API và Mongo. Mục tiêu ở đó không
phải một con số đơn lẻ, mà là tìm điểm bão hoà — nơi req/s phẳng lại hoặc p99 tăng vọt. Nó ổn định tới 800 concurrent
user, và em theo dõi các cột resource để quy giới hạn: latency tăng cùng với CPU API hay Mongo thành ràng buộc.

Em thành thật về các cảnh báo: vài endpoint cho error rate cao dưới test cô lập là artifact của test-setup (kỳ vọng
auth/data), và đây là con số chạy một lần, nên em đọc chúng như tín hiệu capacity, không phải kinh thánh. Nhưng giá
trị là cụ thể: em biết các đường DB-bound bão hoà trước, em biết đại khái ở đâu, và em có percentile thay vì trung
bình — cái đó quan trọng vì trung bình giấu tail latency.

Bài học chủ yếu là kỷ luật đo đạc — nhìn p99 không phải mean, tìm điểm bão hoà một cách có chủ đích, và tương quan
latency với resource metric để biết *vì sao* nó chậm, không chỉ rằng nó chậm."

### 4. Interview Hooks
- "load-test với k6" → *Bạn thiết kế test thế nào? Cô lập vs thực tế?*
- "19K req/s health vs 4.4K vocabulary" → *Vì sao khoảng cách? Nút thắt là gì?*
- "p99 < 32 ms" → *Vì sao p99 không phải trung bình? Hành vi tail thế nào?*
- "ổn định tới 800 concurrent user" → *Bạn tìm bão hoà thế nào? Vượt đó thì gì hỏng?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Bạn cố học gì từ load testing?
2. (L1) Con số health và vocabulary nói gì cho bạn?
3. (L2) Bạn thiết kế bộ test thế nào (cô lập vs workflow)?
4. (L2) Vì sao đo p99/p95 thay vì latency trung bình?
5. (L2) "Điểm bão hoà" nghĩa là gì và bạn tìm nó thế nào?
6. (L3) Vì sao health 19K nhưng vocabulary chỉ 4.4K — thời gian đi đâu?
7. (L3) Bạn tương quan latency với nguyên nhân (CPU vs DB) thế nào?
8. (L3) Bạn cấu hình gì trong k6 — VU, pacing, ramp?
9. (L4) Vài endpoint cho error rate 40–50% — đó là gì và có thật không?
10. (L4) Chuyện gì xảy ra ngay sau điểm bão hoà — nó hỏng thế nào?
11. (L5) Bạn tin con số chạy một lần cỡ nào? Giới hạn là gì?
12. (L5) Throughput vs latency — bạn suy luận về trade-off thế nào?
13. (L6) Trong prod, p99 tăng vọt nhưng throughput ổn — bạn điều tra thế nào?
14. (L6) Bạn phân biệt vấn đề cạn connection-pool với bão hoà CPU thế nào?
15. (L7) Bạn cần 5x capacity — dựa trên kết quả này bạn tối ưu gì trước?
16. (L7) Làm sao bạn biết là DB chứ không phải GC hay scheduler của Go ở concurrency cao?
17. (L8) Bạn có thêm caching cho vocabulary query không? Nó thay đổi gì?
18. (L8) Bạn làm benchmark đáng tin đủ để gate release thế nào?
19. (L8) k6 vs wrk vs Gatling/JMeter — vì sao k6?
20. (L8) Bạn load-test đường gRPC search, không chỉ HTTP, thế nào?

### 6. Câu Trả Lời Chuẩn
1. "Giới hạn thật của API — throughput tối đa mỗi endpoint, tail latency, và chỗ nó bão hoà dưới tải thực tế — để quyết định scaling và capacity dựa trên dữ liệu, không phải đoán."
2. "Health không làm việc thật, nên ~19K là trần runtime/framework. Vocabulary đụng MongoDB, nên ~4.4K là trần việc-thật. Khoảng cách là chi phí round-trip DB và query — nó nói em rằng đường DB là ràng buộc, không phải bản thân Go."
3. "Hai tầng: test cô lập per-endpoint (VU cố định, không pacing) để tìm capacity thô của mỗi endpoint, và một test workflow ramp concurrency qua hành trình user thật để tìm điểm bão hoà hệ thống. Capacity vs hành-vi-dưới-tải."
4. "Vì trung bình giấu tail — một mean tốt vẫn có thể nghĩa là vài user có trải nghiệm tệ. p99 nói em cái mà 1% chậm nhất thấy, đó là thứ thật sự hiện ra thành 'app cảm giác chậm'. Percentile là câu chuyện latency thành thật."
5. "Nó là chỗ req/s phẳng lại dù bạn thêm tải, hoặc p99 bắt đầu tăng vọt — hệ thống không làm được nhiều việc hữu ích hơn, nó chỉ queue. Bạn tìm nó bằng cách ramp concurrency và theo dõi cái inflection đó trong khi tương quan với CPU/bộ nhớ."
6. "Round-trip DB: lấy connection, query Mongo, và deserialize. Health trả ngay; vocabulary trả I/O và việc. Nên khoảng cách ~4x về cơ bản là cái giá của việc đụng database mỗi request."
7. "Em ghi CPU/bộ nhớ API và Mongo trong lúc ramp, nên khi latency tăng em thấy được liệu CPU API bị đóng đinh (compute-bound) hay Mongo là ràng buộc (DB-bound). Tương quan latency với các cột resource là cách quy nguyên nhân chậm."
8. "Virtual user (concurrency), thời lượng test, có pacing (think-time) hay không, và profile ramp. Test cô lập là no-pacing max-throughput; test workflow ramp VU theo từng stage để vẽ đường cong bão hoà."
9. "Chúng chủ yếu là artifact test-setup — endpoint cần auth/data mà script không thoả mãn đầy đủ — không phải server đổ. Em gọi ra điều đó thành thật; nó nghĩa là các con số throughput cụ thể đó kém đáng tin, nên em dựa vào các con số sạch."
10. "Vượt bão hoà, throughput plateau và latency leo khi request queue — cuối cùng bạn nhận error từ timeout hoặc cạn connection-pool/CPU. Nó degrade bằng queue rồi error, không phải một vách sạch, đó là vì sao theo dõi p99 bắt nó sớm."
11. "Không mấy khi đứng riêng — một run, một máy, tổng hợp. Em coi chúng là tín hiệu capacity và mức độ lớn, không phải release gate. Để tin chúng em muốn chạy lặp và confidence interval."
12. "Chúng trade-off sau bão hoà: dưới nó bạn đẩy throughput với latency ổn; tới gần nó, thêm tải mua queue (latency cao hơn) không phải throughput hơn. Điểm vận hành hữu ích là dưới đầu gối nơi latency còn phẳng."
13. "p99 tăng với throughput phẳng gợi ý nguồn tail — GC pause, một dependency chậm, lock contention, hay chờ connection-pool — chứ không phải quá tải thô. Em sẽ xem GC/trace metric và DB pool wait time, vì mean ổn loại trừ quá tải toàn cục."
14. "Cạn pool hiện ra là latency tăng trong khi CPU *không* bão hoà — request chờ một connection rảnh. Bão hoà CPU hiện ra là CPU đóng đinh. Tương quan resource tách 'đang chờ' khỏi 'đang làm', cùng tín hiệu em dùng trong sự cố Kafka."
15. "Dựa trên kết quả này, đường DB — nên caching vocabulary query nóng, tune query/index Mongo, và connection pool. Health chứng minh tầng app còn headroom, nên 5x đến từ loại bỏ round-trip DB, không phải tối ưu Go."
16. "Vì khi Mongo là ràng buộc, CPU API không đóng đinh và resource metric của Mongo động đậy; nếu là GC/scheduler, em sẽ thấy nó trong Go runtime metric (GC pause, số goroutine) với DB rảnh. Em sẽ xác nhận bằng pprof thay vì giả định."
17. "Caching vocabulary (nó khá tĩnh, dữ liệu tham chiếu) sẽ đẩy endpoint đó về phía trần health bằng cách loại round-trip DB khi cache hit — thắng lớn. Trade-off là cache invalidation, nhưng dữ liệu từ điển thay đổi hiếm, nên là một fit mạnh."
18. "Chạy lặp với confidence interval, một corpus và workload đại diện cố định, hardware pin, và một ngưỡng regression — rồi nó gate được. Bây giờ chúng là một-run, nên chúng chẩn đoán, không phải gating; làm chúng gate chủ yếu là về khả năng lặp lại."
19. "k6 script bằng JS, có ngữ nghĩa ramp/threshold tốt, và mô hình VU và scenario sạch, hợp cả test cô lập lẫn workflow của em. wrk nhẹ hơn nhưng ít script được; Gatling/JMeter nặng hơn. k6 là fit tốt nhất cho tải có script, theo stage."
20. "k6 hỗ trợ gRPC, nên em sẽ script các call streaming/unary tới search service, ramp concurrency, và theo dõi latency percentile và bộ nhớ của engine — cùng methodology, khác protocol, và em sẽ tương quan với resource use của engine C++ để tìm bão hoà của nó."

### 7. Whiteboard Version
Vẽ một **đường cong throughput vs concurrency** — nó tăng rồi phẳng lại, và đánh dấu **đầu gối là điểm bão hoà**. Phủ
một **đường cong latency p99** phẳng rồi tăng vọt ở cùng đầu gối — một bức tranh đó là cả câu chuyện. Ở một bên, liệt
**hai loại test** (capacity cô lập, workflow ramp thực tế) và **hai kết quả then chốt** (health ~19K = trần app,
vocabulary ~4.4K = trần DB). Vẽ **các hộp API và Mongo với đồng hồ CPU/bộ nhớ** để cho thấy bạn tương quan latency với
resource use. Người phỏng vấn ngắt ở "vì sao p99 không phải trung bình" (tail latency) và "vì sao vocabulary thấp hơn
nhiều" (round-trip DB). Chủ động nói cảnh báo error-rate.

### 8. Lỗi Thường Gặp
- **"Latency trung bình thấp, nên nó nhanh."** Giấu tail. Tốt hơn: "Em nhìn p99 vì trung bình giấu 1% chậm."
- **"Nó làm 19K req/s"** (trích con số health như capacity của app). Gây hiểu lầm. Tốt hơn: "19K là trần không-làm-gì; endpoint DB-bound thật làm ~4.4K."
- **Lờ error rate.** Nghe như bạn không đọc kết quả của chính mình. Tốt hơn: chủ động giải thích chúng là artifact test-setup.

### 9. Red Flags
- Trích 19K req/s như "throughput của API" — làm rõ đó là endpoint health rỗng, kẻo nghe phóng đại.
- Error rate 40–50% trên vài endpoint nằm trong dữ liệu của chính bạn — thừa nhận chúng là artifact test trước khi người khác tìm ra.
- Con số chạy một lần — khung là tín hiệu capacity, không phải đảm bảo; nói "API gánh 800 user trong production" là nói quá một kết quả phòng lab.

### 10. Kịch Bản Chốt
"Em load-test API với k6 vì em muốn giới hạn thật, không phải đoán. Em chạy hai loại test: test capacity cô lập
per-endpoint và một workflow ramp thực tế. Endpoint health làm khoảng 19K req/s — đó là trần không-làm-gì cho Go và
Gin — trong khi endpoint vocabulary, vốn thật sự query Mongo, làm khoảng 4.4K với p99 dưới 32 mili-giây. Khoảng cách
đó là cả điểm mấu chốt: nó định lượng chi phí round-trip DB và nói em rằng đường database bão hoà trước. Rồi em ramp
một workflow hỗn hợp thực tế từ 10 tới 800 concurrent user, ghi CPU và bộ nhớ API và Mongo để quy bất kỳ chậm nào về
đúng thành phần; nó ổn định tới 800. Em nhìn p99 thay vì trung bình vì trung bình giấu tail, và em thành thật rằng đây
là con số một-run với vài endpoint cho artifact error test-setup — nên em đọc chúng như tín hiệu capacity. Nếu em cần
5x nó, dữ liệu nói caching đường vocabulary, không phải tối ưu Go. Em sẵn lòng đào vào cách làm các con số này đáng
tin đủ để gate release."

---

## Bullet K4 — Lập lịch spaced-repetition SM-2 (deck per-user, khám phá từ mới lọc theo JLPT)

> *"Implemented SM-2 spaced-repetition scheduling with per-user decks and JLPT-filtered new-word discovery."*
> *(từ phần Projects)*

### 1. Ý đồ (Intent)
Đây là bullet **implement thuật toán + tư duy sản phẩm**. Nó cho thấy bạn lấy một thuật toán đã biết (SM-2) và
implement nó đúng với state per-user thật, cộng ra các quyết định sản phẩm hợp lý (lọc JLPT cho từ mới). Ấn tượng:
*người này implement được một thuật toán stateful một cách chính xác và nối nó với một kết quả sản phẩm học tập thật.*

### 2. Elevator Pitch (20–30 giây)
"Em implement phần lập lịch spaced-repetition — cái quyết định khi nào bạn nên ôn một từ lần tới. Em dùng thuật toán
SM-2, cái kinh điển kiểu Anki: dựa trên bạn nhớ một thẻ tốt cỡ nào, nó điều chỉnh một ease factor và đẩy lần ôn tới
xa hơn, nên từ dễ quay lại hiếm và từ khó quay lại sớm. Nó per-user, nên mỗi người có deck và tiến độ riêng, và khám
phá từ mới được lọc theo cấp JLPT để bạn học từ phù hợp với trình độ mình."

### 3. Bản Sâu (1–2 phút)
"Spaced repetition là cơ chế học lõi, nên lập lịch đúng thật sự quan trọng cho việc app có hiệu quả về mặt sư phạm hay không.

Em implement SM-2, thuật toán SuperMemo-2 mà Anki phổ biến hoá. Ý tưởng: mỗi thẻ có một ease factor và một interval.
Khi bạn ôn, bạn chấm điểm mức nhớ, và SM-2 cập nhật ease factor lên hoặc xuống dựa trên điểm đó và nhân interval —
nên một thẻ nhớ rõ có thể đi từ 1 ngày lên 6 ngày lên vài tuần, trong khi một thẻ bạn fail reset về một interval ngắn.
Toán đơn giản nhưng tính đúng đắn nằm ở quản lý state: bạn phải lưu bền ease factor, interval, số lần lặp, và ngày ôn
tới per-thẻ per-user, và cập nhật chúng nhất quán mỗi lần ôn.

Đó là vì sao nó là deck per-user — lịch hoàn toàn cá nhân; cùng một từ 'đến hạn ngày mai' với user này và 'đến hạn
sau một tháng' với user khác dựa trên lịch sử của họ. Nên state lập lịch được key per-user per-thẻ, và 'cái gì đến
hạn bây giờ' là một query trên ngày ôn tới.

Khám phá từ mới lọc theo JLPT là tầng sản phẩm trên đó: khi một user cần thẻ mới, em không kéo vocabulary ngẫu nhiên —
em lọc theo cấp JLPT (các cấp trình độ tiếng Nhật chuẩn hoá, N5 xuống N1) để một người mới học nhận từ N5, không phải
từ nâng cao mù mờ. Nó giữ tài liệu mới ở đúng độ khó.

Kết quả: một lịch ôn cá nhân hoá thích nghi với mức nhớ của từng user cộng giới thiệu từ mới đúng trình độ. Bài học
chủ yếu là thuật toán là phần dễ — việc thật là mô hình hoá state per-user sạch sẽ và làm 'cái gì đến hạn' thành một
query hiệu quả."

### 4. Interview Hooks
- "spaced-repetition SM-2" → *SM-2 hoạt động thế nào? Vì sao SM-2 hơn các cái khác?*
- "deck per-user" → *Bạn mô hình hoá/lưu state thẻ per-user thế nào? Query 'đến hạn' thế nào?*
- "khám phá từ mới lọc theo JLPT" → *Bạn chọn từ mới thế nào? Vì sao lọc theo JLPT?*
- "lập lịch" → *State gì per-thẻ? Bạn tính interval tới thế nào?*

### 5. Câu Hỏi Đào Sâu
1. (L1) Spaced repetition là gì và vì sao nó giúp học?
2. (L1) SM-2 làm gì ở mức cao?
3. (L2) Bạn lưu state gì per-thẻ per-user?
4. (L2) Bạn query "thẻ nào đến hạn bây giờ" hiệu quả thế nào?
5. (L2) Lọc JLPT chọn từ mới thế nào?
6. (L3) Dẫn tôi qua cập nhật SM-2 trên một lần ôn (ease factor, interval).
7. (L3) Bạn xử lý một thẻ fail/again thế nào — hành vi reset?
8. (L3) Bạn giới thiệu thẻ mới vs ôn trong một phiên thế nào — tỷ lệ?
9. (L4) Một user ôn cùng một thẻ hai lần nhanh (double submit) — lịch có hỏng không?
10. (L4) Vấn đề clock/timezone — bạn định nghĩa "đến hạn hôm nay" cho user toàn cầu thế nào?
11. (L5) SM-2 vs một scheduler hiện đại như FSRS — trade-off?
12. (L5) Hàng per-user-per-thẻ vs tính lịch on-the-fly — trade-off lưu trữ?
13. (L6) User than ôn dồn lại sau một kỳ nghỉ — bạn xử lý backlog thế nào?
14. (L6) Một bug lập lịch được ship và interval sai — bạn sửa state hiện có thế nào?
15. (L7) Hàng triệu user × hàng nghìn thẻ — query "đến hạn" còn scale không?
16. (L7) Bạn cache hoặc precompute deck đến hạn thế nào để bắt đầu phiên nhanh?
17. (L8) Bạn có A/B test tham số scheduler không? Thế nào?
18. (L8) Bạn migrate user từ SM-2 sang một thuật toán tốt hơn thế nào mà không mất tiến độ?
19. (L8) Bạn cá nhân hoá ease/interval vượt SM-2 thuần thế nào?
20. (L8) Bạn xử lý leech (thẻ user cứ fail hoài) thế nào?

### 6. Câu Trả Lời Chuẩn
1. "Ôn tài liệu ở các interval tăng dần ngay trước khi bạn sắp quên — nó khai thác spacing effect để bạn nhớ nhiều hơn với ít ôn tổng hơn. Bạn thấy thứ khó thường xuyên và thứ dễ hiếm."
2. "SM-2 theo dõi, per-thẻ, bạn nhớ nó dễ cỡ nào và lập lịch lần ôn tới tương ứng — chấm mức nhớ, nó điều chỉnh một ease factor và kéo dài hoặc thu ngắn interval. Nhớ tốt đẩy lần ôn tới xa hơn; một fail kéo nó về sớm."
3. "Ease factor, interval hiện tại, số lần lặp, và ngày ôn tới — per-user per-thẻ. Bộ bốn đó là toàn bộ state lập lịch; mọi thứ khác suy ra từ nó."
4. "Là một query trên ngày-ôn-tới <= now cho user đó, được index trên (user, next_review_date). Nên 'đến hạn bây giờ' là một index range scan, không phải một phép tính trên mọi thẻ."
5. "Khám phá từ mới lọc pool vocabulary theo cấp JLPT của user và loại các thẻ đã có trong deck, nên thẻ mới đúng trình độ và chưa thấy. Nó giữ độ khó khớp với người học."
6. "Khi ôn: nếu nhớ tốt, ease factor được nhích (giới hạn bởi một sàn), và interval mới là interval cũ × ease factor (với vài bước đầu cố định, kiểu 1 rồi 6 ngày); nếu nhớ kém, số lần lặp reset và interval tụt về một giá trị ngắn. Rồi em lưu bền interval, ease, số lần lặp, và ngày ôn tới mới."
7. "Một fail reset số lần lặp và interval về giá trị ngắn/ban đầu nên thẻ quay lại nhanh, và ease factor bị phạt nên nó sẽ lớn chậm hơn lần sau. Thẻ phải được kiếm lại về các interval dài."
8. "Một phiên trộn ôn đến hạn với một số thẻ mới có cap, nên bạn không bị ngập — ôn là ưu tiên (chúng nhạy thời gian), và thẻ mới lấp phần capacity còn lại tới một giới hạn ngày. Cái cap đó là thứ giữ khối lượng công việc hợp lý."
9. "Nó không nên — một lần ôn áp một transition kiểu idempotent dựa trên điểm, và một double submit hoặc bị dedup bởi bản ghi ôn hoặc chỉ áp hai lần; để an toàn em key lần ôn để lịch cập nhật một lần mỗi lần ôn có chủ ý. Bảo vệ cập nhật là cách sửa."
10. "Định nghĩa 'đến hạn' tương đối với ngày cục bộ của user, lưu timestamp bằng UTC và áp timezone của user cho ranh giới ngày. Nếu không 'đến hạn hôm nay' mơ hồ qua các timezone và thẻ hiện/biến ở các giờ lạ."
11. "SM-2 đơn giản, hiểu rõ, và đủ tốt; FSRS chính xác hơn vì nó mô hình hoá trí nhớ với nhiều tham số hơn và fit theo dữ liệu. Em dùng SM-2 cho đơn giản và vì nó đã được kiểm chứng; FSRS sẽ là bản nâng cấp nếu em muốn độ chính xác data-driven, đổi lại phức tạp."
12. "Lưu hàng per-user-per-thẻ làm 'đến hạn' thành một query index rẻ và giữ lịch sử; tính on-the-fly tiết kiệm lưu trữ nhưng làm 'cái gì đến hạn' đắt và khó-stateless. Cho một app ôn thì pattern query chi phối, nên em lưu state."
13. "Cap tải ôn ngày và/hoặc lập lịch lại backlog để nó không đổ hết một lúc — dồn giết động lực. Bạn trải các thẻ quá hạn thay vì trình hàng trăm thẻ một phiên; nó là một quyết định sản phẩm cũng như lập lịch."
14. "Vì state là hàng per-thẻ, em có thể tính lại hoặc sửa các trường bị ảnh hưởng trong một migration — và vì em lưu số lần lặp và ease, em thường xây lại được ngày ôn tới đúng từ chúng thay vì reset tiến độ của user. Lưu full state là thứ làm cách sửa không-phá-huỷ."
15. "Có, vì 'đến hạn' là một query index (user, next_review_date), nên nó scale per-user bất kể tổng kích thước — bạn chỉ bao giờ fetch thẻ đến hạn của một user. Index là thứ giữ nó phẳng; không có nó bạn sẽ scan mọi thứ."
16. "Precompute hoặc cache deck đến hạn của mỗi user cho ngày hiện tại nên bắt đầu phiên là một lần đọc, refresh khi các lần ôn đến. Vì tính-đến-hạn chỉ thay đổi khi ôn hoặc sang ngày, nó rất cacheable."
17. "Có — nhóm user và biến các tham số như điều chỉnh ease hay cap thẻ-mới, rồi so sánh retention và tải ôn. Metric là retention vs công sức; bạn cần đủ user và thời gian để nó có ý nghĩa."
18. "Map state per-thẻ hiện có (interval, ease, số lần lặp) vào mô hình của thuật toán mới như một warm start thay vì reset — đa số scheduler seed được từ lịch sử ôn. Giữ tiến độ là cả ràng buộc, và state đã lưu làm nó khả thi."
19. "Cá nhân hoá điều chỉnh ease hay interval per-user dựa trên độ chính xác quan sát của họ — thực chất fit độ hung hăng theo cách họ thật sự làm, hướng mà FSRS chính thức hoá. SM-2 thuần dùng hằng số cố định; cá nhân hoá làm chúng thích nghi."
20. "Phát hiện thẻ fail lặp lại (một ngưỡng leech trên số lapse), rồi flag hoặc suspend chúng để chúng ngừng lấn át ôn, và đưa chúng ra để giúp thêm. Lưu số lần lặp/lapse chính xác là thứ cho phép em phát hiện leech."

### 7. Whiteboard Version
Vẽ **vòng đời của một thẻ** như một timeline: ôn → chấm → SM-2 cập nhật (ease factor, interval) → lần ôn tới đẩy xa;
một fail tụt nó về một interval ngắn. Rồi vẽ **hàng state thẻ per-user** (user, thẻ, ease, interval, số lần lặp,
next_review_date) và khoanh tròn index trên `(user, next_review_date)` — "đây là thứ làm 'đến hạn bây giờ' rẻ." Ở một
bên, vẽ **khám phá từ mới**: pool vocabulary → lọc theo cấp JLPT → loại đã thấy → thẻ mới. Người phỏng vấn ngắt ở "làm
sao query cái gì đến hạn ở quy mô" (index) và "SM-2 vs FSRS" (đơn giản vs chính xác). Giữ nó ngắn — đây là bullet nhẹ
nhất của bạn.

### 8. Lỗi Thường Gặp
- **"Em tính lịch on-the-fly mỗi lần."** Mở đường cho nghi ngờ về scaling. Tốt hơn: "State lưu per-thẻ; 'đến hạn' là một query index."
- **"SM-2 chỉ là nhân interval."** Quá nông. Tốt hơn: nhắc ease factor, reset khi fail, và các bước đầu cố định.
- **"Timezone không quan trọng."** Có quan trọng cho 'đến hạn hôm nay'. Tốt hơn: lưu UTC, ranh giới ngày cục bộ của user.

### 9. Red Flags
- Đây là bullet nhẹ nhất, "thuật toán đã biết" nhất — đừng bán quá như nó mới lạ. Khung nó thành "implement SM-2 đúng với state per-user sạch", vừa thành thật vừa vững.
- Sẵn sàng thật sự giải thích cập nhật SM-2; vung tay về một thuật toán có tên bạn nói mình implement là một hình ảnh xấu.
- "lọc theo JLPT" — biết JLPT là gì (các cấp trình độ N5–N1) để nó không nghe như một buzzword bạn mượn.

### 10. Kịch Bản Chốt
"Em implement phần lập lịch spaced-repetition, đó là cơ chế học lõi — nó quyết định khi nào bạn ôn mỗi từ lần tới. Em
dùng SM-2, thuật toán kinh điển kiểu Anki: mỗi thẻ có một ease factor và một interval, và khi bạn chấm mức nhớ nó điều
chỉnh ease và kéo dài interval, nên từ dễ quay lại hiếm và từ fail reset về một interval ngắn. Bản thân thuật toán đơn
giản; việc thật là state — em lưu ease, interval, số lần lặp, và ngày ôn tới per-user per-thẻ, và 'cái gì đến hạn bây
giờ' chỉ là một query index trên ngày ôn tới, cái đó giữ nó rẻ kể cả khi deck lớn. Nó hoàn toàn per-user, nên cùng một
từ đến hạn ngày mai với người này và tháng sau với người khác. Trên đó, khám phá từ mới lọc theo cấp JLPT nên người học
nhận từ đúng trình độ thay vì vocabulary ngẫu nhiên. Nếu thú vị em có thể so sánh SM-2 với thứ như FSRS, hay nói về
cách em xử lý ôn dồn sau khi user nghỉ một thời gian."

---

# Chủ đề xuyên suốt (đan chúng qua bất kỳ bullet nào)

Đây là các tín hiệu lặp lại làm cả câu chuyện của bạn mạch lạc. Người phỏng vấn thưởng cho *sự nhất quán*:

1. **Idempotency thay vì exactly-once.** PnL, referral, thông báo — bạn liên tục chọn at-least-once + ghi idempotent
   thay vì săn exactly-once ở tầng transport. Nói nó theo cùng một cách mỗi lần; nó nghe như một nguyên tắc thật, được đúc kết.
2. **Tính đúng đắn nằm gần lưu trữ.** Ràng buộc duy nhất (referral, rút), commit-sau-khi-lưu (PnL) — bạn đẩy các đảm bảo
   xuống DB, không phải các check tầng-app có thể race.
3. **Chẩn đoán trước khi scale.** Sự cố Kafka và công việc k6 đều cho thấy "CPU rảnh trong khi lag/latency tăng = bạn
   đang chờ, không phải đang làm." Cái bản năng chẩn đoán đó nối các câu chuyện vận hành của bạn lại.
4. **Chọn đúng primitive.** Sorted set cho ranking, state machine cho edge-trigger, ports/adapters cho swappability,
   client-streaming cho bulk. Bạn với tay lấy đúng công cụ, không brute-force.
5. **Thành thật về giới hạn.** Các cảnh báo benchmark (một-run, tổng hợp, dị thường 2-thread-chậm-hơn) là một điểm cộng,
   không phải điểm trừ — chủ động nói ra chúng báo hiệu senior mạnh hơn nhiều so với một con số lớn.
