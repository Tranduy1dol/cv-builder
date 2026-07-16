# GLM 5.2

## Kết quả

**Level: Mid-level Backend Engineer (Go)** — phù hợp 3 năm kinh nghiệm. Có tiềm năng Senior nếu đi sâu thêm distributed systems edge cases và production operations.

---

### Điểm mạnh

| Area | Assessment |
| ---- | ---------- |
| **Debugging methodology** | Chuẩn đoán lag 4M bài bản: UI symptom → DB → ArgoCD/log → Kafka UI → format message → bottleneck DB round-trip |
| **Practical optimization** | Batch 1000 + group userID/symbol + bulk insert; có benchmark chọn batch size; đề xuất đổi format producer |
| **Idempotency awareness** | `ON CONFLICT DO NOTHING` theo transaction id, DLQ sau retry 3 lần, event_id tracking trong transaction |
| **Trade-off awareness** | Batch vs per-message, make-vs-buy search engine, gRPC vs cgo — trả lời thẳng thắn khi chưa đào sâu benchmark |
| **Architecture intuition** | Shard state theo symbol, preload DB + Kafka replay khi recovery, tách Firebase worker khỏi pipeline chính |

---

### Điểm cần cải thiện

| Area | Feedback |
| ---- | -------- |
| **Consistency khi trả lời** | Partition key lúc nói `symbol` (price-alert), lúc nói `userid` (PnL) — chưa reconcile conflict giữa 2 use case trong cùng hệ thống |
| **Edge case depth** | Cùng timestamp-ms, late arrival cross-partition, graceful shutdown khi batch đang chạy vượt `terminationGracePeriodSeconds` — trả lời còn surface |
| **Production operations** | Monitoring metrics, backpressure, capacity planning được hỏi nhưng chưa đi sâu / chưa trả lời đủ |
| **Consumer group config** | Chưa nêu rõ `enable.auto.commit`, `max.poll.interval.ms`, `session.timeout.ms` khi bị hỏi trực tiếp |
| **Benchmark honesty gap nhỏ hơn session 2** | Kotoba Press: thừa nhận chưa đào sâu bottleneck ở 19K req/s — tốt, nhưng cần chủ động nêu limitation sớm hơn |

---

### Recommendations để lên Senior:

1. **Reconcile conflicting constraints sớm** — khi hệ thống có 2 nhu cầu ordering khác nhau (per-user PnL vs per-symbol alert), tự nêu conflict và đề xuất tách pipeline / dual topology trước khi bị hỏi ngược
2. **Practice SIGTERM end-to-end** — drain batch với context timeout, partial commit + replay semantics, và cách không mất offset 501–1000
3. **Own production ops vocabulary** — lag alert thresholds, backpressure (pause poll), partition scaling vs consumer scaling trade-offs
4. **Self-critique khi trình bày** — mỗi giải pháp nêu sẵn gap (hot partition, in-memory loss on crash) thay vì đợi follow-up

---

### Score breakdown (1-5)

| Criteria | Score |
| -------- | ----- |
| Problem-solving | 4/5 |
| Technical depth | 3/5 |
| System design | 3/5 |
| Communication | 3.5/5 |
| Production maturity | 3/5 |
| Overall | 3.3/5 |

---
## Câu 1

**Q: Bạn hãy chọn một trong hai dự án chính — RaidenX hoặc Kotoba Press — và trình bày trong 2-3 phút: (1) vai trò cụ thể, (2) bài toán khó nhất, (3) quyết định kỹ thuật tự hào nhất.**

A: Dự án RaidenX là dự án em tham gia lâu nhất, trong dự án này em đảm nhận pipeline xử lý dữ liệu từ kafka, và xây dựng các đầu api tổng hợp cho user trả về UI. Trong dự án này em tự migrate sang version cũ serve 1 blockchain viết bằng typescript sang serve trên nhiều blockchain viết bằng golang, ngoài ra em tối ưu pipeline xử lý dữ liệu sau sự cố lag 4 triệu message xuống mức vài nghìn và ổn định ở mức đó

---
## Câu 2 (Follow-up - Root cause & Batching)

**Q: Bạn nhắc đến tối ưu pipeline từ 4M lag xuống vài nghìn. Trong CV ghi chuyển từ per-message (1:1) sang batched processing.**
1. **Khi xảy ra 4M backlog, bước đầu tiên xác định root cause? Làm sao biết bottleneck ở xử lý 1:1 chứ không phải CPU/RAM hay DB write chậm?**
2. **Batch như thế nào (theo thời gian, số lượng, hay key)? Nếu batch quá lớn, rủi ro gì?**

A: vấn đề xảy ra khi user giao dịch nhưng không có thông tin vị thế trả về trên UI. Em chuẩn đoán có thể là DB write có vấn đề, hoặc consumer xử lý dữ liệu có vấn đề. Thì kiểm tra DB chưa có dữ liệu, em tiếp tục xem dashboard argoCD và consumer log. Lúc này dashboard báo consumer instance healthy và log không có error. Em chuyển sang xem kafka UI để xem có message trả về chưa và lag như thế nào. Lúc này em thấy có message rồi nhưng lag khá lớn. Kết luận vấn đề là do tốc độ xử lý của consumer không đuổi kịp producer nên mới lag cao như vậy. Tiếp tục kiểm tra format message và logic pipeline. format message là 1 transaction / message, còn pipeline xử lý như sau: lấy transaction -> làm sạch loại bỏ data rác -> tính toán -> insert db -> commit offset. với số lượng nhiều transaction mà transaction nào cũng phải đi full pipeline, em nhận định đây là vấn đề cốt lõi và suy nghĩ cách giải quyết. về consumer side là phía em own, em thực hiện batch 1000 message, group theo userID/symbol và sort theo timestamp-ms được provide trong message, sau đó mới làm sạch cả batch và bulk insert vào DB. cuối cùng thì mới commit offset. ngoài ra em đề xuất thay đổi phía producer về format message, thay vì 1 thì đổi thành n transaction/message. về trade off ở phía em sẽ là latency cao hơn do có thao tác DB write khá lớn, phía producer thì cũng phải nặng hơn về logic. cả 2 phía chấp nhận trade-off và implement, thấy lag giảm dần và ổn định ở mức vài nghìn. em kết luận đã fix thành công về con số 1000 batch size, đây là kết quả của quá trình benchmark. nếu nhỏ hơn 1000 thì lag giảm không đáng kể, tốn nhiều thời gian hơn để drain lượng lag đang có, nếu lớn hơn thì DB write sẽ thành bottleneck mới

---
## Câu 3 (Follow-up - Ordering & Idempotency)

**Q: Khi batch 1000 messages, group theo user/symbol và sort theo timestamp-ms:**
1. **Cùng timestamp-ms chính xác: làm sao đảm bảo thứ tự PnL đúng (open rồi mới close)?**
2. **Late arrival: message từ partition khác của cùng user đến sau với timestamp cũ hơn batch đã commit — xử lý thế nào?**
3. **Bulk insert fail ở message thứ 500: retry cả batch? Idempotency cho 499 records đã insert thế nào?**

A: 1. về case này, ở bước group em có dedup theo transaction id, nếu có nhiều message có cùng transaction id thì dedup lấy message mới nhất. em cũng đề xuất phía producer trả message kafka vào patrition theo key là userID để đảm bảo ordering nhưng không được accept. nên phương án tốt nhất em có thể làm lúc đó là group theo userID, symbol và sort theo timestamp-ms được provide trong message. 2. về case này, em sẽ đánh dấu là position cần rebuild, và để 1 backgroup job recompute lại position data theo order. 3. case này em sẽ retry toàn bộ batch, sử dụng insert on conflict do nothing với điều kiện conflict là transaction id. insert thành công mới commit offset. setup retry limit 3 lần và có dlq handle riêng với những batch failed retry để investigate sau

---
## Câu 4 (Chuyển topic - Kotoba Press)

**Q: CV ghi tự build full-text search engine C++ (15K docs/sec indexing, p50=0.04ms at 30K qps) và load test 19K req/s.**
1. **Make vs Buy: tại sao tự viết C++ thay vì Elasticsearch/Meilisearch/Postgres FTS? Đặc thù tiếng Nhật là gì?**
2. **Tại sao gRPC thay vì cgo gọi C++ trực tiếp từ Go? Lợi ích và rủi ro?**
3. **Ở 19K req/s (health), bottleneck tiếp theo nằm ở đâu?**

A: 1 ngoài lý do là để hiểu hơn cấu trúc của 1 search engine, thì bản thân em nghĩ các công cụ elasticsearch sẽ hơi overhead. khi dự án scale lên em sẽ cân nhắc tích hợp lại. Bản thân tiếng nhật là ngôn ngữ tượng hình, các kí tự có nhiều sự khác biệt và khó hơn ở phase tokenizer, nên thuận tiện nhất em muốn custom tokenizer đi kèm với nó là indexer riêng. 2. về lựa chọn grpc, grpc cung cấp static type protocol, hỗ trợ giao tiếp internal service binary gọn nhẹ và tốc độ cao hơn, ngoài ra cung cấp contract để các service dùng chung. Nhưng bản thân phía search engine khi code grpc cũng khó hơn, tốn nhiều thời gian để kiểm thử. 3. về câu hỏi này em chưa đào sâu khi thực hiện benchmark, nhưng em đoán thì bottleneck tiếp theo có thể là DB read write, chưa có tách read và write path

---
## Câu 5 (Follow-up - Price Alert Scaling)

**Q: Price-alert state machine — giả sử 1 triệu user active, mỗi user 5–10 alerts, price ticks 50K–100K msg/s qua Kafka.**
1. **State machine đặt ở đâu (memory / Redis / DB)? Làm sao check điều kiện không thành bottleneck O(N) với ~10M alerts?**
2. **Khi match, đẩy Kafka → Firebase. Làm sao latency < 1s mà không nghẽn vì Firebase chậm/rate limit?**

A: 1, kiến trúc state machine em đặt state trong memory sử dụng hashmap, để check điều kiện alert thì có logic group theo userid/token, kiểm tra điều kiện và kiểm tra user setting, sau đó mới check state dựa trên giá hiện tại và state trong memory để fire notify. user setting cũng được lưu trong memory và được update thông qua pub/sub. 2. để tránh rate limit hoặc spam, thì em có cache thêm 2 redis value: 1 đại diện cho khả năng gửi message, thường ttl trong 2s, khi value này còn tồn tại thì không gửi noti, 2 là count noti để gửi thông báo gộp, detail noti vẫn được persist vào db nên user vẫn có thể xem trong api. để tránh trường hợp firebase bị nghẽn thì em sẽ xử lý bất đồng bộ, tạo 1 process riêng như consumer kafka chỉ để fire noti qua firebase/websocket

---
## Câu 6 (Follow-up - In-memory State & Horizontal Scaling)

**Q:**
1. **Scale nhiều instance: làm sao message giá của token BTC đi đúng instance đang giữ state alert cho BTC?**
2. **Pod crash / OOM: state memory mất — recovery thế nào để không sai hoặc spam lại notify?**

A: 1. cái này sẽ shard state theo routing key, ở đây dùng symbol làm key. cũng sẽ setup patrition key theo symbol kafka, vì consumer sẽ chỉ handle 1 patrition nên vẫn đảm bảo state. 2. về recovery, em có làm preload from db trước khi run consumer, kafka sẽ replay từ offset mới nhất được commit. replay notify thì sẽ cần idempotent để không spam, resend notify

---
## Câu 7 (Graceful Shutdown & Offset / Partition)

**Q: Khi K8s gửi SIGTERM tới Go Kafka consumer:**
1. **Graceful shutdown: stop poll → drain batch → commit → leave group? Nếu batch > terminationGracePeriodSeconds (30s) thì sao?**
2. **Rebalance khi consumer leave: message lost/duplicate? At-least-once?**
3. **Idempotency khi replay offset sau kill force?**
4. **Partition key: symbol vs userid? Hot partition?**
5. **Offset commit: per-message vs cả batch — trade-off?**

A: graceful shutdown: quy trình sẽ là ngưng nhận message mới, xử lý nốt message cũ, commit offset, leave consumer group, shut down. nếu slowquery thì có thể chia nhỏ batch thành nhiều batch bé hơn. rebalance: trigger khi thấy consumer leave group, khi broker kiểm tra heartbeat mà timeout, không có tín hiệu thì sẽ force rebalance, nhưng leave group vẫn là phương án tốt và chủ động hơn. quá trình rebalance theo quy trình leave group/graceful shutdown vẫn có commit offset, message nào chưa handle thì có thể replay. idempotency: vẫn phải dùng insert on conflict do nothing, ngoài ra có thể wrap vào 1 transaction với 1 query insert event id, replay thêm logic kiểm tra event id sẽ đảm bảo hơn. patrition key: ý tưởng tốt nhất vẫn là userid, nếu dùng symbol sẽ spawn số lượng lớn patrition, ngoài ra trong trường hợp có patrition sẽ tối ưu consumer, consumer vẫn hoạt động thì không phải shard, để shard patrition cần thêm 1 ordering provider như thêm sequencer number vào từng message, lúc này consumer phải thêm logic merge và order lại. Offset Commit Strategy: nếu mà per-message persistent rồi mới commit, throughput giảm hẳn do phải thực hiện nhiều db write, nhưng commit đến đâu là hoàn thành đến đó, ở hướng ngược lại batch message persistent với bulk insert chỉ tốn 1 db query nhưng khi error phải retry toàn bộ.

---
## Câu 8 (Follow-up cuối — chưa trả lời trong session)

**Q:**
1. **Graceful shutdown + partial batch:** K8s grace period 30s, batch đang xử lý tới message 500 rồi timeout — offset commit đến 500, message 501–1000 lost hay replay? Duplicate thế nào?
2. **Partition key mâu thuẫn:** trước đó shard alert theo **symbol**, sau lại nói key tốt nhất là **userid**. PnL cần ordering per-user, price-alert cần grouping per-symbol — giải quyết conflict thế nào?
3. **Consumer config ngắn:** `enable.auto.commit` true/false vì sao? `max.poll.interval.ms` set bao nhiêu để không bị kick khi xử lý batch 1000?

A: _(chưa trả lời trong session)_
