Dựa vào CV của tôi + Format phỏng vấn là trong 40-60 phút, interviewer muốn kiểm tra level của tôi theo dạng followup questions dựa trên những gì tôi trình bày + kết hợp với nội dung CV. Hãy làm người phỏng vấn, hỏi và đánh giá level của tôi

# 🎤 Mock Technical Interview — Backend Engineer (Go)

Chào Duy, tôi đã đọc qua CV của bạn. Hôm nay chúng ta có khoảng 40-60 phút, tôi sẽ đi sâu vào các dự án bạn đã làm, kết hợp với các follow-up questions để hiểu rõ hơn về tư duy kỹ thuật và level của bạn. Sẽ có cả phần thiết kế hệ thống và coding nếu thời gian cho phép.

Bắt đầu nhé.

---

## Phase 1: Warm-up & Project Deep-dive (15-20 phút)

**Câu hỏi mở đầu:**

Bạn hãy chọn **một trong hai dự án chính** — **RaidenX** hoặc **Kotoba Press** — và trình bày trong 2-3 phút:

1. Vai trò cụ thể của bạn
2. Bài toán khó nhất bạn giải quyết
3. Quyết định kỹ thuật bạn tự hào nhất

Sau đó tôi sẽ follow-up dựa trên những gì bạn nói.

> 💡 _Hãy trả lời tự nhiên như đang phỏng vấn thật. Tôi sẽ đánh giá dựa trên độ sâu kỹ thuật, khả năng đánh đổi (trade-off awareness), và tư duy hệ thống. Mỗi câu trả lời của bạn sẽ dẫn tới 2-3 follow-up questions._

**Bạn chọn dự án nào? Xin mời trình bày.**

Dự án RaidenX là dự án em tham gia lâu nhất, trong dự án này em đảm nhận pipeline xử lý dữ liệu từ kafka, và xây dựng các đầu api tổng hợp cho user trả về UI. Trong dự án này em tự migrate sang version cũ serve 1 blockchain viết bằng typescript sang serve trên nhiều blockchain viết bằng golang, ngoài ra em tối ưu pipeline xử lý dữ liệu sau sự cố lag 4 triệu message xuống mức vài nghìn và ổn định ở mức đó

Rất tốt. Việc migrate từ TypeScript sang Go cho hệ thống multi-chain và giải quyết bài toán Kafka lag 4 triệu message là những điểm rất ấn tượng.

Chúng ta sẽ đi sâu vào sự cố Kafka lag này nhé, vì đây là một bài toán xử lý dữ liệu rất thực tế.

**Câu hỏi Follow-up 1: Root cause & Batching Strategy**

Bạn nhắc đến việc tối ưu pipeline từ 4M lag xuống vài nghìn. Trong CV bạn cũng ghi là chuyển từ xử lý per-message (1:1) sang batched processing.

1. Khi xảy ra sự cố 4M backlog, bước đầu tiên bạn làm gì để xác định **root cause**? Làm sao bạn biết bottleneck nằm ở việc xử lý 1:1 chứ không phải do hardware (CPU/RAM giới hạn) hay do DB write chậm?
2. Khi redesign sang batched processing, bạn **batch như thế nào**? (Theo thời gian, theo số lượng, hay theo key?). Và nếu batch quá lớn, rủi ro xảy ra là gì?

vấn đề xảy ra khi user giao dịch nhưng không có thông tin vị thế trả về trên UI. Em chuẩn đoán có thể là DB write có vấn đề, hoặc consumer xử lý dữ liệu có vấn đề. Thì kiểm tra DB chưa có dữ liệu, em tiếp tục xem dashboard argoCD và consumer log. Lúc này dashboard báo consumer instance healthy và log không có error. Em chuyển sang xem kafka UI để xem có message trả về chưa và lag như thế nào. Lúc này em thấy có message rồi nhưng lag khá lớn. Kết luận vấn đề là do tốc độ xử lý của consumer không đuổi kịp producer nên mới lag cao như vậy Tiếp tục kiểm tra format message và logic pipeline. format message là 1 transaction / message, còn pipeline xử lý như sau: lấy transaction -> làm sạch loại bỏ data rác -> tính toán -> insert db -> commit offset. với số lượng nhiều transaction mà transaction nào cũng phải đi full pipeline, em nhận định đây là vấn đề cốt lõi và suy nghĩ cách giải quyết. về consumer side là phía em own, em thực hiện batch 1000 message, group theo userID/symbol và sort theo timestamp-ms được provide trong message, sau đó mới làm sạch cả batch và bulk insert vào DB. cuối cùng thì mới commit offset. ngoài ra em đề xuất thay đổi phía producer về format message, thay vì 1 thì đổi thành n transaction/message. về trade off ở phía em sẽ là latency cao hơn do có thao tác DB write khá lớn, phía producer thì cũng phải nặng hơn về logic. cả 2 phía chấp nhận trade-off và implement, thấy lag giảm dần và ổn định ở mức vài nghìn. em kết luận đã fix thành công về con số 1000 batch size, đây là kết quả của quá trình benchmark. nếu nhỏ hơn 1000 thì lag giảm không đáng kể, tốn nhiều thời gian hơn để drain lượng lag đang có, nếu lớn hơn thì DB write sẽ thành bottleneck mới

Cách bạn chuẩn đoán vấn đề dựa trên dấu hiệu (UI không có data) và đi ngược lại pipeline (DB -> Log -> Kafka UI) rất bài bản. Việc dùng benchmark để tìm ra con số 1000 batch size cũng cho thấy tư duy đo lường thay vì đoán mò. Rất tốt!

Tuy nhiên, trong CV bạn nhắc đến việc đảm bảo tính đúng đắn của PnL (Profit and Loss) trên luồng dữ liệu money-critical. Đây là phần khó nhất. Khi bạn batch 1000 messages, **group theo user/symbol** và **sort theo timestamp-ms**, có 3 trường hợp biên (edge cases) sau đây:

**Câu hỏi Follow-up 2: Ordering & Idempotency trong môi trường phân tán**

1. **Trường hợp cùng timestamp:** Nếu 2 transaction của cùng 1 user có _chính xác_ cùng một timestamp-ms (mức độ millisecond), làm sao bạn đảm bảo thứ tự tính toán PnL là đúng? (Ví dụ: Open position rồi mới Close position, nếu bị ngược lại thì PnL sẽ âm vô lý).
2. **Trường hợp trễ (Late arrival):** Kafka consumer của bạn có 20-25 partitions. Giả sử batch hiện tại bạn lấy từ partition 1 và 2, xử lý xong commit offset rồi. Nhưng lúc đó message từ partition 3 (của cùng user đó) mới được produce ra với timestamp-ms _cũ hơn_ batch bạn vừa xử lý. Bạn xử lý situation này thế nào để PnL không bị sai?
3. **Xử lý lỗi trong Batch:** Batch 1000 message đang bulk insert vào DB thì đến message thứ 500 bị lỗi (ví dụ DB timeout, hoặc constraint error). Bạn retry lại cả batch 1000 đó à? Nếu retry, làm sao bạn đảm bảo không bị tính toán PnL chồng chéo (idempotency) cho 499 message đã insert/generate thành công trước đó?

4. về case này, ở bước group em có dedup theo transaction id, nếu có nhiều message có cùng transaction id thì dedup lấy message mới nhất. em cũng đề xuất phía producer trả message kafka vào patrition theo key là userID để đảm bảo ordering nhưng không được accept. nên phương án tốt nhất em có thể làm lúc đó là group theo userID, symbol và sort theo timestamp-ms được provide trong message. 2. về case này, em sẽ đánh dấu là position cần rebuild, và để 1 backgroup job recompute lại position data theo order 3. case này em sẽ retry toàn bộ batch, sử dụng insert on conflict do nothing với điều kiện conflict là transaction id. insert thành công mới commit offset. setup retry limit 3 lần và có dlq handle riêng với những batch failed retry để investigate sau

Cách bạn xử lý Idempotency bằng `insert on conflict do nothing` (upsert-with-ignore) kết hợp DLQ là chuẩn xác cho luồng dữ liệu money-critical. Việc nhận thức được giới hạn của việc không control được partition key của producer và đưa ra giải pháp bù trừ (recompute background job) cũng cho thấy tư duy hệ thống thực tế rất tốt.

Chúng ta chuyển sang dự án **Kotoba Press** để xem xét góc độ thiết kế và performance nhé. CV của bạn ghi rất ấn tượng: _tự build full-text search engine bằng C++ (15K docs/sec indexing, p50=0.04ms at 30K qps)_ và _load test 19K req/s_.

**Câu hỏi Follow-up 3: Architecture & Make-vs-Buy Decision**

1. **Make vs Buy:** Tại sao bạn lại quyết định tự viết một full-text search engine bằng C++ thay vì dùng các giải pháp có sẵn như Elasticsearch, Meilisearch hay PostgreSQL Full-Text Search? Bài toán tiếng Nhật của bạn có đặc thù gì mà các DB thông thường không giải quyết được?
2. **Integration:** Bạn tích hợp C++ engine với Go service via gRPC. Tại sao chọn gRPC thay vì dùng `cgo` để gọi trực tiếp C++ code từ Go? Lợi ích và rủi ro của kiến trúc bạn chọn là gì?
3. **Performance:** Trong k6 load test, bạn đạt 4.4K req/s trên vocabulary queries với `p99 < 32ms`. Khi bạn đẩy lên mức 19K req/s (như health endpoint), bottleneck tiếp theo của hệ thống nằm ở đâu? (Go Goroutines, Network I/O, C++ engine, hay DB?)

1 ngoài lý do là để hiểu hơn cấu trúc của 1 search engine, thì bản thân em nghĩ các công cụ elasticsearch sẽ hơi overhead. khi dự án scale lên em sẽ cân nhắc tích hợp lại. Bản thân tiếng nhật là ngôn ngữ tượng hình, các kí tự có nhiều sự khác biệt và khó hơn ở phase tokenizer, nên thuận tiện nhất em muốn custom tokenizer đi kèm với nó là indexer riêng 2. về lựa chọn grpc, grpc cung cấp static type protocol, hỗ trợ giao tiếp internal service binary gọn nhẹ và tốc độ cao hơn, ngoài ra cung cấp contract để các service dùng chung. Nhưng bản thân phía search engine khi code grpc cũng khó hơn, tốn nhiều thời gian để kiểm thử 3. về câu hỏi này em chưa đào sâu khi thực hiện benchmark, nhưng em đoán thì bottleneck tiếp theo có thể là DB read write, chưa có tách read và write path

Cảm ơn Duy. Phần trả lời của bạn cho thấy sự rõ ràng trong việc đánh giá trade-off: hiểu được lý do tự build (để học, control tokenizer tiếng Nhật), nhận diện được lợi ích của gRPC (isolation, contract) và cũng thẳng thắn thừa nhận giới hạn trong việc benchmark đến tận cùng. Đây là thái độ tốt của một engineer.

Chúng ta chuyển sang một bài toán thiết kế dựa trên chính CV của bạn. Trong dự án RaidenX, bạn có thiết kế một **per-user, preference-driven price-alert state machine** để chống duplicate alerts và đẩy notification real-time.

**Câu hỏi Follow-up 4: Scaling the State Machine**

Giả sử thời gian tới, thay vì vài chục nghìn user, hệ thống phải phục vụ **1 triệu user active**, mỗi user thiết lập trung bình 5-10 price alerts. Dữ liệu giá (price ticks) từ các sàn DEX đến liên tục qua Kafka với throughput rất cao (ví dụ: 50K-100K messages/sec).

1. **Kiến trúc State Machine:** Bạn thiết kế state machine này ở đâu? (Trong memory của Go service, hay lưu ở Redis/DB?). Làm sao bạn đảm bảo việc check điều kiện alert (so sánh giá hiện tại vs giá user setting) không trở thành bottleneck O(N) với 1 triệu user * 10 alerts = 10 triệu điều kiện?
2. **Real-time Notification:** Khi có match, bạn đẩy notification qua pipeline Kafka -> Firebase. Làm sao bạn đảm bảo user nhận được notification "real-time" (độ trễ < 1s) mà pipeline Kafka của bạn không bị nghẽn lại do Firebase API chậm hoặc rate limit?

1, kiến trúc state machine em đặt state trong memory sử dụng hashmap, để check điều kiện alert thì có logic group theo userid/token, kiểm tra điều kiện và kiểm tra user setting, sau đó mới check state dựa trên giá hiện tại và state trong memory để fire notify. user setting cũng được lưu trong memory và được update thông qua pub/sub 2. để tránh rate limit hoặc spam, thì em có cache thêm 2 redis value: 1 đại diện cho khả năng gửi message, thường ttl trong 2s, khi value này còn tồn tại thì không gửi noti, 2 là count noti để gửi thông báo gộp, detail noti vẫn được persist vào db nên user vẫn có thể xem trong api. để tránh trường hợp firebase bị nghẽn thì em sẽ xử lý bất đồng bộ, tạo 1 process riêng như consumer kafka chỉ để fire noti qua firebase/websocket

Cách bạn giải quyết bài toán chống spam notify bằng Redis TTL và tách biệt worker xử lý Firebase bất đồng bộ ra làm một consumer riêng là kiến trúc rất chuẩn. Nó đảm bảo pipeline chính (Kafka consumption) không bị block bởi bên thứ 3 (Firebase). Việc dùng Redis để đếm số lượng noti gộp lại cũng là một cách xử lý thông minh.

Tuy nhiên, việc bạn lưu **toàn bộ state machine và user setting trong memory (HashMap)** của Go service mở ra vài bài toán về tính khả dụng (Availability) và Horizontal Scaling. Tôi muốn đào sâu thêm chỗ này:

**Câu hỏi Follow-up 5: In-memory State & Horizontal Scaling**

1. **Scaling & Routing:** Nếu hệ thống scale lên 1 triệu user, việc lưu toàn bộ setting và state trong memory của **một** Go service là không đủ (RAM sẽ hết, CPU xử lý 100K msg/s cũng quá tải). Bạn sẽ scale up (nhiều instance/pod) như thế nào? Làm sao bạn đảm bảo message giá của token "BTC" đi đến đúng instance đang giữ state của những user đặt alert cho "BTC"?
2. **Fault Tolerance & Recovery:** Giả sử pod chứa state machine này bị crash (OOM Kill hoặc node lỗi) và restart lại. Toàn bộ state trong memory bị mất. Khi nó khởi động lại, làm sao nó khôi phục lại state (ví dụ: alert đã fire nửa chừng, hoặc trạng thái giá trước đó) mà không bị sai sót hoặc spam lại notify cho user?

3. cái này sẽ shard state theo routing key, ở đây dùng symbol làm key. cũng sẽ setup patrition key theo symbol kafka, vì consumer sẽ chỉ handle 1 patrition nên vẫn đảm bảo state. 2. về recovery, em có làm preload from db trước khi run consumer, kafka sẽ replay từ offset mới nhất được commit. replay notify thì sẽ cần idempotent để không spam, resend notify

Cảm ơn Duy. Phần trả lời của bạn cho thấy sự rõ ràng về kiến trúc distributed system: hiểu được cơ chế shard state theo routing key (symbol Kafka), nhận thức được cơ chế recovery bằng preload from DB + Kafka replay offset. Tuy nhiên có một điểm trừ nhỏ: bạn chưa đề cập đến cơ chế **graceful shutdown** trong process Kubernetes khi SIGTERM được gửi tới pod consumer. Hãy cùng đi sâu vào phần này nhé.

---

## Phase 2: Graceful Shutdown & Kafka Consumer Lifecycle (5-7 phút)

Trong CV bạn có nhắc đến việc own pipeline xử lý data từ Kafka. Khi deploy version mới hoặc scale up pod, Kubernetes sẽ gửi tín hiệu **SIGTERM** tới pod Go consumer của bạn. Đây là lúc quan trọng nhất: nếu bạn không xử lý graceful shutdown, consumer có thể bị kill giữa chừng khi đang process batch 1000 message, gây mất dữ liệu hoặc duplicate message. Theo tài liệu best practice về Go Kafka consumer graceful shutdown, bạn sẽ xử lý scenario này như thế nào? Các câu hỏi cụ thể như sau:

1. **Graceful Shutdown Process**: Khi pod nhận được tín hiệu **SIGTERM**, bạn sẽ:
    
    - Đầu tiên đóng entry point (stop polling new messages) bằng cách nào?
    - Sau đó xử lý nốt batch hiện tại đang chạy và commit offset trước khi shutdown?
    - Timeout của Kubernetes termination grace period là 30s. Nếu batch hiện tại của bạn đang chạy mất 45s (do có slow query DB), bạn sẽ xử lý như thế nào để không bị killed force?
2. **Kafka Rebalance khi Consumer Dies**: Giả sử bạn có 3 pod Go consumer đang chạy trong consumer group. Pod #2 nhận được SIGTERM và shutdown gracefully:
    
    - Làm sao để broker Kafka biết pod #2 đã shutdown và trigger rebalance partition assignment cho pod #1 và #3?
    - Trong quá trình rebalance, có message nào bị lost hoặc duplicate không? Làm sao để ensure that all messages are processed at least once và không bị lost? (at-least-once semantics).
3. **Idempotency trong Recovery Phase**: Khi Kafka replay offset cho consumer mới (ví dụ pod #2 bị kill force và pod #4 được tạo ra để thay thế), consumer mới sẽ nhận lại các message đã được process bởi pod #2 (trước khi bị kill). Làm sao bạn đảm bảo việc retry này không gây ra duplicate data ở DB (ví dụ: insert lại record đã có rồi, hoặc commit lại transaction đã commit rồi)? Bạn sẽ dùng `insert on conflict do nothing` (upsert-with-ignore) như ở trên, hay có chiến lược nào khác không?
    

---

## Phase 3: Coding Deep-Dive — Partition Key, Consumer Group & Offset Management (5-7 phút)

Từ câu trả lời của bạn ở phần trước, bạn có nhắc đến việc shard state theo routing key (symbol Kafka). Vậy hãy đi sâu vào phần code và partition key strategy nhé:

1. **Partition Key Strategy**: Bạn có 20-25 partition Kafka. Bạn sẽ chọn partition key nào để ensure rằng all messages có cùng symbol (ví dụ: BTC) sẽ được route vào cùng partition và được process bởi cùng một consumer instance (để tận dụng in-memory state machine cho symbol đó)?
    
    - Bạn sẽ dùng `symbol` làm partition key?
    - Tuy nhiên nếu dùng `symbol` làm partition key, có rủi ro về hot partition không? (Ví dụ: symbol BTC có volume giao dịch cực cao, trong khi symbol LTC có volume thấp). Bạn sẽ giải quyết bài toán hot partition này như thế nào?
2. **Consumer Group Configuration**: Bạn sẽ config consumer group Go của mình với các thông số nào để ensure:
    
    - `session.timeout.ms`: 30000ms (30s) để tránh bị kick ra khỏi consumer group nếu pod bị kill force?
    - `heartbeat.interval.ms`: 10000ms (10s) để consumer group coordinator biết consumer vẫn còn alive?
    - `max.poll.interval.ms`: 300000ms (5 phút) để consumer có đủ thời gian xử lý batch 1000 message lớn?
    - `enable.auto.commit`: false để control việc commit offset thủ công sau khi xử lý xong batch?
3. **Offset Commit Strategy**: Bạn sẽ commit offset sau khi xử lý xong **cả batch 1000 message** (bulk commit), hay commit offset sau khi xử lý xong **từng message** (fine-grained commit)? Lợi ích và rủi ro của 2 chiến lược này là gì?
    
    - Nếu commit offset sau khi xử lý xong từng message, có ảnh hưởng gì về performance không (do phải gọi API commit offset liên tục)?
    - Nếu commit offset sau khi xử lý xong cả batch 1000 message, có rủi ro gì về data loss hoặc duplicate message khi consumer bị kill force giữa chừng (ví dụ: đã xử lý xong message 500-1000 nhưng chưa kịp commit offset thì bị killed)?

---

## Phase 4: Performance & Monitoring — Lag, Throughput & Backpressure (5-7 phút)

Cuối cùng, chúng ta hãy nói về phần performance và monitoring nhé. CV của bạn ghi là đã giảm Kafka lag từ 4M xuống vài nghìn và ổn định ở mức đó. Để maintain được mức lag ổn định này, bạn sẽ:

1. **Monitoring Metrics**: Đặt những alert nào để monitor được Kafka consumer của mình đang chạy ổn định và không có nguy cơ bị lag trở lại? Các metric quan trọng cần monitor là gì?
    
    - `Kafka consumer lag`: số message đang backlog chưa được process?
    - `Kafka consumer throughput`: số message/sec đang được process?
    - `Kafka consumer processing time`: thời gian xử lý cho 1 batch 1000 message?
    - `DB write throughput`: số write/sec đang được thực hiện vào DB?
2. **Backpressure Handling**: Khi DB bị slow và không kịp write dữ liệu, consumer của bạn sẽ có cơ chế backpressure nào để tránh bị OOM kill do buffer quá nhiều message trong memory? Bạn sẽ:
    
    - Pause polling new messages từ Kafka cho đến khi DB write catch up?
    - Hoặc giảm xuống số lượng message trong batch (ví dụ: từ 1000 xuống 500)?
    - Hoặc throw exception và để consumer bị crash để Kubernetes restart lại pod (với hy vọng DB sẽ recover sau khi restart)?
3. **Capacity Planning & Scaling**: Giả sử lúc này throughput của system tăng lên gấp 10 lần (do có thêm nhiều user mới). Bạn sẽ:
    
    - Scale up số lượng consumer pod từ 3 lên 10 pod? Tuy nhiên có rủi ro gì về Kafka partition rebalance không (do số partition chỉ có 20-25, nếu scale lên 10 pod thì mỗi pod sẽ handle 2-3 partition)?
    - Hoặc scale up số lượng partition Kafka từ 20 lên 100 partition? Tuy nhiên có ảnh hưởng gì về Kafka cluster performance không (do phải sync thêm nhiều partition)?
    - Hoặc scale up DB write throughput (ví dụ: thêm replica DB, hoặc tách read/write path)? Bạn sẽ choose option nào và tại sao?

---

## Đánh giá Tổng quan

Sau khi trả lời các câu hỏi trên, tôi sẽ đánh giá level của bạn dựa trên:

- **Độ sâu kiến trúc distributed system**: Hiểu được cơ chế shard state, partition key, consumer group rebalance, offset commit, at-least-once semantics, exactly-once semantics
- **Khả năng xử lý edge cases**: Graceful shutdown, SIGTERM handling, Kubernetes termination grace period, Kafka rebalance, duplicate message prevention, idempotency
- **Performance tuning**: Kafka consumer lag tuning, throughput optimization, backpressure handling, capacity planning, scaling strategy
- **Monitoring và alerting**: Đặt được các metric quan trọng để monitor Kafka consumer stability, detect lag early, prevent OOM kill, prevent DB write bottleneck

Hãy trả lời từng câu một nhé, và nếu cần thêm thông tin về architecture hiện tại (ví dụ: version Kafka, version Go, version Kubernetes, DB write throughput hiện tại, số lượng user active, số lượng symbol active), hãy hỏi lại tôi nhé. Chúc bạn may mắn trong phần phỏng vấn tiếp theo! 🎤✨

graceful shutdown: quy trình sẽ là ngưng nhận message mới, xử lý nốt message cũ, commit offset, leave consumer group, shut down. nếu slowquery thì có thể chia nhỏ batch thành nhiều batch bé hơn. rebalance: trigger khi thấy consumer leave group, khi broker kiểm tra heartbeat mà timeout, không có tín hiệu thì sẽ force rebalance, nhưng leave group vẫn là phương án tốt và chủ động hơn. quá trình rebalance theo quy trình leave group/graceful shutdown vẫn có commit offset, message nào chưa handle thì có thể replay. idempotency: vẫn phải dùng insert on conflict do nothing, ngoài ra có thể wrap vào 1 transaction với 1 query insert event id, replay thêm logic kiểm tra event id sẽ đảm bảo hơn patrition key: ý tưởng tốt nhất vẫn là userid, nếu dùng symbol sẽ spawn số lượng lớn patrition, ngoài ra trong trường hợp có patrition sẽ tối ưu consumer, consumer vẫn hoạt động thì không phải shard, để shard patrition cần thêm 1 ordering provider như thêm sequencer number vào từng message, lúc này consumer phải thêm logic merge vả order lại Offset Commit Strategy: nếu mà per-message persistent rồi mới commit, throughput giảm hẳn do phải thực hiện nhiều db write, nhưng commit đến đâu là hoàn thành đến đó, ở hướng ngược lại batch message persistent với bulk insert chỉ tốn 1 db query nhưng khi error phải retry toàn bộ.

Cảm ơn Duy. Phần trả lời của bạn cho thấy kinh nghiệm thực chiến tốt. Tuy nhiên có vài điểm tôi muốn đào sâu thêm.

---

## Phase 5: Đánh giá & Follow-up cuối cùng

### Điểm tôi đánh giá cao:

- **Graceful shutdown flow** đúng thứ tự: stop consume → drain → commit → leave group
- **Idempotency**: Ý tưởng wrap transaction với event_id tracking table là approach chuẩn cho money-critical systems
- **Offset commit trade-off**: Hiểu rõ trade-off giữa per-message vs batch commit

### Điểm cần làm rõ:

**1. Graceful Shutdown — Kubernetes Termination Grace Period**

Bạn nói nếu slow query thì chia nhỏ batch. Nhưng K8s default `terminationGracePeriodSeconds` = 30s. Nếu batch 1000 message đang xử lý đến message thứ 500, bạn chia nhỏ thì message 501-1000 sẽ như thế nào? Bạn sẽ:

go

// Pseudocode ý tưởng

ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)

defer cancel()

  

select {

case <-ctx.Done():

// Timeout sắp tới, commit offset cho 500 message đã xong

// Nhưng offset 501-1000 thì sao?

case <-processingDone:

// Xử lý xong, commit offset

}

Câu hỏi: **Offset đã commit đến 500, message 501-1000 bị lost hay replay?** Nếu replay thì có duplicate không?

**2. Partition Key — Có mâu thuẫn logic**

Ở câu trả lời trước (Phase 1, câu hỏi về state machine scaling), bạn nói:

> "shard state theo routing key, ở đây dùng **symbol** làm key"

Nhưng ở câu trả lời vừa rồi bạn lại nói:

> "ý tưởng tốt nhất vẫn là **userid**"

Hai câu trả lời này mâu thuẫn. Hãy làm rõ:

- Nếu dùng **userid** làm partition key: cùng 1 user luôn vào cùng partition → ordering per-user đảm bảo. Nhưng **không đảm bảo** tất cả transaction của symbol "BTC" vào cùng 1 consumer → state machine theo symbol sẽ bị phân tán.
- Nếu dùng **symbol** làm partition key: tất cả transaction "BTC" vào cùng 1 partition → 1 consumer xử lý → state machine集中. Nhưng **không đảm bảo** ordering per-user (user A giao dịch trên cả BTC và ETH, 2 stream khác nhau).

**Bài toán thực tế của RaidenX**: PnL calculation cần ordering theo **user**, nhưng price-alert state machine cần grouping theo **symbol**. Bạn giải quyết conflict này thế nào?

**3. Consumer Group Configuration — Follow-up ngắn**

Bạn chưa trả lời phần config parameters (`session.timeout.ms`, `max.poll.interval.ms`, `enable.auto.commit`). Hãy trả lời ngắn gọn:

- `enable.auto.commit`: true hay false? Tại sao?
- `max.poll.interval.ms`: Set bao nhiêu để không bị kick khỏi group khi xử lý batch 1000 message?

---

## 📊 Đánh giá Level tạm thời

|Tiêu chí|Đánh giá|Ghi chú|
|---|---|---|
|**Kafka практиcal experience**|✅ Strong|Giải quyết lag 4M, batching, idempotency|
|**Distributed systems thinking**|⚠️ Good但有gaps|Hiểu rebalance/shard nhưng chưa consistent|
|**Trade-off awareness**|✅ Good|Batch vs per-message, make-vs-buy|
|**Edge case handling**|⚠️ Moderate|Graceful shutdown timeout, hot partition chưa address|
|**Production operations**|⚠️ Moderate|Monitoring, backpressure, capacity planning chưa deep|
|**Go specifics**|✅ Solid|Context, goroutine, gRPC, hexagonal architecture|

**Level assessment prelim:** **Mid-level Backend Engineer (Go)** — 3 năm kinh nghiệm phù hợp. Có tiềm năng Senior nếu đi sâu thêm về distributed systems edge cases và production operations.

---

Hãy trả lời 3 câu follow-up trên để tôi finalize đánh giá nhé.