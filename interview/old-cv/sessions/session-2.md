
# Claude Sonet 5

## Kết quả

**Level: Mid-level Backend Engineer, với một số điểm chạm senior ở mảng distributed systems reliability.**

### Điểm mạnh

- **Debugging methodology rất tốt**: cả 2 case (Kafka lag 4M và p99 spike do CPU throttle) đều theo đúng quy trình loại trừ có hệ thống — kiểm tra DB → health → metrics → drill down đúng chỗ. Không đoán mò, luôn có bằng chứng trước khi kết luận.
- **Tư duy correctness/idempotency thuộc dạng tốt hơn mặt bằng chung 3 năm kinh nghiệm**: outbox pattern, `ON CONFLICT DO NOTHING` với composite key, và đặc biệt là chuyển từ `ZINCRBY` (non-idempotent) sang `ZADD` + sequence number (idempotent-by-design) — đây là insight mà nhiều người có 5+ năm vẫn không tự nghĩ ra, phải bị dồn vào chân tường mới thấy.
- **Trung thực khi bị hỏi ngược về benchmark Kotoba Press**: không cố bảo vệ con số trên CV, giải thích rõ context (5000 docs, in-memory). Interviewer thật sự đánh giá cao điều này hơn là một câu trả lời "chống chế".
- **Hiểu đúng nguyên nhân gốc** (root cause) thay vì chỉ thuộc "triệu chứng — cách fix": ví dụ GOMAXPROCS vs cgroup quota, hay giải thích _tại sao_ batching giảm lag (amortize DB round-trip).

### Điểm cần cải thiện

- **Go runtime internals còn mỏng**: câu hỏi về `context.WithTimeout` cho thấy bạn biết _cách dùng đúng_ (pass ctx, select trên Done) nhưng ban đầu chưa phân biệt rõ "context bảo vệ ai" — đây là kiến thức nền tảng, nên đọc lại kỹ phần cooperative cancellation, scheduler (goroutine không bị force-kill).
- **Chưa chủ động nêu trade-off/rủi ro trước khi bị hỏi**: ở câu outbox relay (câu 5), bạn cần được dẫn dắt mới nhận ra pattern outbox tự nó cũng có race condition. Ở level senior thật sự, người ta thường tự "self-critique" giải pháp của mình ngay khi trình bày, không đợi interviewer hỏi ngược.
- **CV có xu hướng liệt kê số liệu thiếu context** (không chỉ Kotoba Press) — nên rà lại toàn bộ CV, mỗi con số impressive nên có 1 dòng chú thích ngắn về điều kiện đo, để tránh bị đọc là "phóng đại".
 
---
## Câu 1

**Q: Bạn hãy giới thiệu ngắn gọn (2-3 phút) về bản thân, và trong 3 năm ở Sotatek, dự án nào bạn cho là mình đóng góp/impact lớn nhất, vì sao?**

A: em chào anh/chị, em là trần mạnh duy, tốt nghiệp trường đại học công nghệ đại học quốc gia hà nội. Em có 3 năm kinh nghiệm làm backend ở sotatek. Các hệ thống em được tham gia chủ yếu là hệ thống giao dịch crypto. Trong đó raidenx là dự án em tham gia lâu nhất. trong dự án này em đảm nhận phần xử lý dữ liệu, consumer side và xây dựng đầu api tổng hợp dữ liệu giao dịch trên nhiều blockchain cho user. impact lớn nhất là em tự migrate từ version cũ là typescript sang golang và tối ưu consumer side, giảm lag sau sự cố từ khoảng 4 triệu message lag xuống mức ổn định khoảng vài ngàn. ngoài ra em tự xây dựng được các tính năng như trả về trending token và xây dựng hệ thống cảnh báo giá sử dụng state machine

---
## Câu 2 (follow-up)

**Q: Khi phát hiện lag 4 triệu message, bạn đã dùng những metric/công cụ nào để chẩn đoán? Làm sao bạn biết được bottleneck nằm ở đâu (consumer xử lý chậm, hay do downstream như Redis/DB chậm, hay do rebalance liên tục, v.v.)?
Bạn nói chuyển từ xử lý per-message (1:1) sang batch giúp tăng throughput ~10x — về mặt cơ chế, tại sao batching lại giúp giảm lag đáng kể như vậy? (Cụ thể là chi phí nào được amortize khi bạn gộp batch — network round-trip, DB write, offset commit, hay gì khác?)**

A: vấn đề xảy ra khi user giao dịch nhưng không có dữ liệu về position. thì em chuẩn đoán theo 2 hướng: db write có vấn đề hoặc consumer hoạt động có vấn đề. để xác minh thì đầu tiên em kiểm tra dữ liệu trong DB -> chưa có. Em tiếp tục xem log và dashboard trên argoCD, thấy pod healthy và consumer không có log error -> em loại trừ được vấn đề về tính khả dụng của consumer. sau đó em xem kafka ui để biết có message đến và lag thì em thấy lag đã lên tới 4M message. mức độ lag này là khá lớn, em kiểm tra format message đến và logic xử lý ở consumer side. format message lúc đó là 1 message 1 transaction. và logic xử lý như sau: message đến -> làm sạch để loại bỏ dữ liệu rác -> tính toán -> persist vào db -> commit offset. với mỗi transaction đến lại phải thực hiện 1 db roundtrip như vậy. Em nhận định vấn đề là ở db roundtrip chứ không phải là CPU chậm. từ đó thực hiện optimize phía consumer: xử lý batch size 1000, group theo userid/symbol và sort theo timestamp-ms được provide trong message. ngoài ra em đề xuất phía crawler thay đổi format message thành n transaction thay vì 1. về kết quả, sau khi thực hiện các thay đổi thì với mỗi 1 lượt đi vào pipeline, có khoảng vài nghìn transaction được xử lý, dù độ trễ có tăng 1 chút khi mà dùng bulk insert nhưng có thể chấp nhận được với tốc độ xử lý mức độ vài nghìn transaction trong 1 round thay vi 1 transaction 1 round

---
## Câu 3 (follow-up 2 phần)

**Q: Bạn group message theo `userId/symbol` trong memory trước khi xử lý để đảm bảo ordering. Nhưng nếu bạn scale consumer lên nhiều instance (mỗi instance chỉ đọc một subset partition do Kafka phân), thì việc group trong memory của một instance liệu có đủ đảm bảo ordering đúng cho một user không? Điều gì xảy ra nếu message của cùng 1 user/symbol bị phân vào 2 partition khác nhau, do 2 consumer instance khác nhau xử lý song song? 
Với batch 1000 message: bạn commit offset ở bước nào trong flow — trước hay sau khi ghi DB? Nếu consumer crash giữa chừng (ví dụ đã insert DB xong nhưng chưa kịp commit offset), khi consumer restart và đọc lại batch đó, làm sao bạn tránh bị double-count PnL cho user? Bạn dùng cơ chế gì để đảm bảo idempotency ở tầng DB write (unique constraint, upsert key, dedup table...)? **

A: khi mà scale lên nhiều instance như vậy có thể không đảm bảo ordering đối với transaction của user. em cũng đã đề xuất cải tiến cài đặt patrition key theo userID, nhưng trade off là tăng logic và giảm throughput. phía producer muốn đẩy vào kafka theo từng topic là network và giữ round-robin patrition để đảm bảo balance nên chưa áp dụng cải tiến này. vì vậy khi xử lý dữ liệu ở consumer, nước đi tốt nhất em có thể làm là group theo userID, và sort theo timestamp-ms được provide trong message để giữ ordering về commit offset, em lựa chọn insert db hoàn tất rồi mới commit, khi insert failed retry toàn batch với số lần nhất định, thường là 3 để latency không lên quá cao. để tránh double count em thực hiện insert on conflict do nothing với điều kiện on conflict là userid và transaction id. em có thêm 1 lớp dedup ở phase group để khi có nhiều hơn 1 transaction id thì sẽ chỉ lấy 1 cái mới nhất

---
## Câu 4

**Q: Bạn có nhắc đến "Redis-backed leaderboard rankings" cũng nằm trong cùng pipeline PnL recompute này. Giả sử flow là: consume batch → tính PnL → ghi DB (idempotent qua `ON CONFLICT DO NOTHING`) → *update leaderboard trên Redis* (ví dụ `ZINCRBY` hoặc `ZADD`) → commit offset.**

**Nếu consumer crash *sau khi update Redis nhưng trước khi commit offset*, khi restart nó sẽ replay lại batch đó. Bước ghi DB thì an toàn nhờ `ON CONFLICT DO NOTHING` (không insert lại). Nhưng bước update Redis thì sao — nó có bị double-count không? Nếu có, bạn xử lý vấn đề "dual-write giữa 2 hệ thống có tính idempotent khác nhau" (DB idempotent, Redis không tự idempotent nếu dùng lệnh increment) như thế nào?**

A: với vấn đề dual write này, em sử dụng outbox transactional, mở rộng lệnh insert thành 1 transaction với 2 query: query 1 insert event id để write vào redis, query 2 insert data vào db. sau khi commit transaction thành công thì sẽ commit offset. cài đặt thêm 1 relay để scan event table, với trường hợp crash sau khi update redis thành công thì sẽ scan thấy event id đã có trong db -> ignore

---
## Câu 5 (tiếp tục đào outbox)

**Q: Relay của bạn: (1) đọc event từ outbox table, (2) ghi vào Redis (`ZINCRBY`/`ZADD`), (3) mark event đó là "processed". Bước (2) và (3) là 2 thao tác trên *2 hệ thống khác nhau* (Redis + Postgres), nên bản thân cặp thao tác này *lại không atomic* — y hệt vấn đề ban đầu bạn đang cố giải quyết.**

**Nếu relay crash *sau khi `ZINCRBY` thành công nhưng trước khi mark processed*, khi relay restart nó sẽ đọc lại event đó (vì chưa mark done) và *`ZINCRBY` lần nữa* → leaderboard bị sai (double count), dù bảng outbox/DB hoàn toàn đúng.**

**Câu hỏi: bạn xử lý gap này thế nào?** 

A: cái này em sẽ dùng zadd để không bị cộng dồn như zincrby, tách phase write redis khỏi pipeline, để việc write này cho relay đảm nhận. cuối cùng có thể gắn sequencer number cho mỗi thao tác, để khi relay quét và tính toán sẽ chỉ áp dụng tính toán cho thao tác có sequencer mới nhất

> với vấn đề dual write này em dùng outbox pattern: mở rộng insert thành 1 transaction gồm 2 query — insert event vào outbox table và insert data vào db, commit xong mới commit offset. có 1 relay scan outbox table để ghi vào redis, sau đó mark event là processed.
> nhưng bản thân relay cũng có vấn đề tương tự: bước ghi redis và bước mark processed là 2 thao tác trên 2 hệ thống khác nhau nên tự nó không atomic. nếu relay crash sau khi ghi redis nhưng trước khi mark processed, restart lên sẽ đọc lại event đó và ghi redis lần 2 → leaderboard sai dù db hoàn toàn đúng.
> để fix gap này em không cố làm relay chạy đúng 1 lần, mà làm cho thao tác ghi redis tự nó idempotent: thay ZINCRBY (cộng dồn) bằng ZADD ghi giá trị tuyệt đối đã tính sẵn, kèm sequence number để chỉ áp dụng nếu sequence mới hơn giá trị đang lưu — ghi lại bao nhiêu lần cũng ra cùng 1 kết quả.

---
## Câu 6

**Q: Trong consumer xử lý batch 1000 message với nhiều partition (~20-25 theo CV), bạn có xử lý song song bên trong 1 consumer instance không (ví dụ dùng goroutine pool để xử lý nhiều batch/nhiều group user cùng lúc), hay xử lý tuần tự theo từng batch nhận được? Nếu có dùng concurrency ở tầng này, bạn kiểm soát số lượng goroutine chạy đồng thời như thế nào để tránh việc goroutine bung ra không kiểm soát (goroutine leak) khi downstream (DB) chậm lại đột ngột?**

A: em nghĩ có thể áp dụng xử lý song song trong 1 consumer instance, bằng cách sử dụng 1 semaphore như 1 worker pool có số lượng giới hạn tùy theo setting hiện tại của pod. để tránh việc goroutine leak thì có thêm context.WithTimeout và defer cancel. ngoài ra em nghĩ chỉ nên concurrency ở bước tính toán, sau khi các goroutine xong việc vả giải phóng hết thì mới insert db, lí do thì để concurrency write vừa tiêu tốn lượng connection đến db, vừa không đảm bảo tốc độ nhanh hơn so với bulk insert

---
## Câu 7 (đào sâu Go concurrency)

**Q: Bạn nói dùng `context.WithTimeout` + `defer cancel()` để tránh goroutine leak. Nhưng bản thân việc tạo context với timeout **không tự động "giết" goroutine** — nó chỉ đóng channel `ctx.Done()`. Goroutine chỉ thực sự dừng nếu code bên trong nó **chủ động `select` trên `ctx.Done()`** và return khi channel đó đóng.

**Vậy giả sử 1 goroutine trong worker pool của bạn đang gọi 1 hàm tính toán CPU-bound thuần túy (không có I/O, không gọi hàm nào nhận `ctx`) — nếu context timeout xảy ra giữa lúc đó, goroutine này có dừng lại không? Nếu không, `context.WithTimeout` trong trường hợp này thực sự đang bảo vệ điều gì?**

A: đầu tiên goroutine không nhận ctx thì sẽ không dừng lại mà sẽ chạy đến khi hoàn thành, vì vậy cần có cơ chế cooperate bằng truyền context vào trong goroutien và select ctx done để biết đã timeout và dừng goroutine lại

> context.WithTimeout thực ra không tự dừng được goroutine, nó chỉ đóng channel ctx.Done() thôi. nếu goroutine bên trong không select trên ctx.Done() — ví dụ nó đang chạy 1 hàm cpu-bound thuần túy không nhận ctx — thì dù timeout xảy ra, goroutine đó vẫn chạy tới khi xong, vẫn leak.
>
> cái context timeout thực sự bảo vệ là phía **caller** — người đang chờ kết quả. nếu em select giữa ctx.Done() và channel nhận result, thì caller sẽ không bị block vô hạn, nó return sớm và move on, dù goroutine con vẫn chạy ngầm phía sau tốn cpu/memory 1 lúc nữa.
>
> để goroutine con thực sự dừng đúng lúc thì phải làm nó cooperative — truyền ctx xuống, chia nhỏ công việc và select ctx.Done() giữa các bước, hoặc nếu là hàm cpu-bound không chia nhỏ được thì buộc phải check ctx.Err() định kỳ trong loop tính toán. Go không có cơ chế force-kill goroutine từ bên ngoài, mọi cancellation đều phải cooperative.

---
## Câu 8

**Q: Bạn tự viết 1 full-text search engine bằng C++, đạt *15K docs/sec indexing* và *p50 = 0.04ms ở 30K qps*. Đây là những con số rất cao (0.04ms = 40 microsecond cho 1 query, ở mức so sánh được với những engine production-grade như Tantivy/Lucene ở workload nhỏ).

**Hỏi thẳng: dataset bạn benchmark có kích thước bao nhiêu (số docs, tổng size)? Và bạn dùng cấu trúc dữ liệu gì để index (inverted index dạng gì — có dùng skip list, FST, hay đơn giản là hashmap + postings list)? Vì nếu p50 = 0.04ms ở mức load 30K qps, điều đó gợi ý toàn bộ index phải nằm gọn trong CPU cache hoặc ít nhất là RAM không có bất kỳ disk I/O nào — bạn có thể mô tả rõ setup đo benchmark này không (single-thread hay multi-thread, warm cache hay cold)?**

A: benchmark có kích thước 5000 docs trong đó trung bình 100 token/docs, inverted index em cài đặt dạng đơn giản gồm hashmap và posting list. benchmark này được run trên github ci. do dataset chưa phải quá lớn nên toàn bộ index được build trong memory, phần lớn dữ liệu nằm ở ram và phần nhỏ là trong cpu cache, không có disk i/o. kết quả p50 với mức load 30k qps được trả ra dưới setup concurrency 4 worker

---
## Câu 9

**Q: Kể một lần cụ thể bạn dùng `pprof` để tìm bottleneck trong production (không nhất thiết phải là incident 4M lag ở trên, có thể là case khác). Cụ thể: bạn dùng CPU profile, heap profile, hay goroutine profile? Bạn nhìn vào đâu trong flame graph / `top` output để xác định function nào là thủ phạm, và sau khi fix thì bạn verify lại bằng cách nào để chắc chắn đã cải thiện đúng chỗ đó (chứ không phải cải thiện chỗ khác rồi lag tự giảm do trùng hợp)?

A: về câu hỏi này có tình huống api workload vẫn hoạt động bình thường, nhưng p99 thi thoảng lại spike lên khá cao. đầu tiên em monitoring xem đầu api nào gặp vấn đề, setting ở pod nào. sau khi thấy setting thì sẽ đi vào log, dashboard, thấy pod vẫn healthy và log không trả ra error, em đi vào xem metrics ở pod đó thì thấy chỉ có p99 tăng, các giá trị p90, p50 vẫn ổn định. em tiếp tục xem cpu stas, các giá trị nr_throttle và nr_period và tính ratio thì thấy kết quả khoảng 30% là mức đáng báo động, vì vấn đề có thể do cpu bị throttle, hết quota kiến kernel dumd các process cho đến chu kì quota tiếp theo, em kiểm tra giá trị gomaxprocs thấy set không đúng với cpu của pod. sau khi sửa lại em tính lại giá trị ratio và xem kết quả p99, p90, p50, thấy các kết quả đã ở mức cho phép thì em xác nhận đã fix thành công

---
## Câu 10

**Q:Khi dùng CPU profile của `pprof` (`go tool pprof`), bạn phân biệt thế nào giữa 1 function có *flat time cao* (tự nó tốn CPU) và 1 function có *cumulative time cao* nhưng flat time thấp (chỉ là "người gọi", CPU thực sự tốn ở hàm nó gọi bên trong)? Tại sao phân biệt 2 khái niệm này quan trọng khi bạn quyết định optimize chỗ nào?

A: theo em hiểu là flat time cao thì tính toán trong nó thực sự tốn cpu, trong khi cumulative time cao nhưng flat thấp thì các hàm nó gọi đến có mức độ tiêu tốn cpu lớn. phân biệt được 2 khái niệm này đưa ra hướng tối ưu hoàn toàn khác nhau, flat cao thì sẽ tối ưu mặt tính toán trong hàm đó, nhưng nếu cum time cao mà flat time thấp thì phải xem lại nó đang gọi đến hàm nào, thay thế hàm con có chi phí rẻ hơn
