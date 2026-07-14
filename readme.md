# "Tell me about yourself" — Trần Mạnh Duy (Go Backend Engineer)

> Câu trả lời "Giới thiệu bản thân" (3–5 phút), viết để **nói ra miệng** trong phỏng vấn — tự nhiên,
> theo dòng thời gian, nối các dự án lại với nhau, dành nhiều thời gian nhất cho phần backend mạnh nhất
> (RaidenX), và chỉ nói vừa đủ để tạo tò mò. Mục tiêu: người phỏng vấn kết thúc với **3–5 chủ đề kỹ
> thuật họ muốn đào sâu**, chứ không phải nghe hết cả sự nghiệp.
>
> Cách dùng: đọc to vài lần cho tới khi nó nghe như *lời của bạn*, không phải học thuộc. Những chỗ in
> đậm là "hook" — cứ để chúng lửng lơ một chút để người phỏng vấn nhảy vào hỏi.

---

## Bản chính (3–5 phút, nói tự nhiên)

"Vâng, để em giới thiệu ngắn gọn. Em là kỹ sư backend, làm chủ yếu với **Go** và các **hệ thống
event-driven**, khoảng 3 năm kinh nghiệm, phần lớn là các nền tảng giao dịch crypto — nơi mà tính đúng
đắn của dữ liệu gần như không được phép sai, vì nó liên quan trực tiếp đến tiền của người dùng.

Em bắt đầu ở Sotatek theo một hướng hơi khác một chút — ban đầu em làm **nghiên cứu blockchain**, viết
**Rust** cho một Layer-2 dựa trên Madara, kiểu Starknet client, và tìm hiểu về hệ thống zero-knowledge
proof. Giai đoạn đó cho em nền tảng về hệ thống phân tán và tư duy 'low-level', nhưng em nhận ra thứ em
thực sự thích là xây dựng các dịch vụ backend chịu tải cao, chạy thật, có người dùng thật — nên em chuyển
hẳn sang làm backend.

Dự án em muốn nói nhiều nhất, và cũng là phần em tự hào nhất, là **RaidenX** — một DEX aggregator đa
chuỗi. Em sở hữu (own) service tên là **`insight`**, tức là toàn bộ phần dữ liệu giao dịch: tính lãi/lỗ
(PnL/ROI) cho từng ví, bảng token đang trending, và cảnh báo giá real-time. Khi em nhận, nó mới chỉ là một
**prototype TypeScript chạy được đúng một chuỗi (Sui)** — và bọn em thì biết chắc là sẽ đi đa chuỗi. Nên
em **viết lại nó bằng Go theo hướng event-driven**, và điểm mấu chốt là em đẩy toàn bộ phần đặc thù của
từng chuỗi ra sau một 'lớp chuẩn hoá' — phía trên lớp đó, một sự kiện chỉ đơn giản là ví, token, số lượng,
chiều mua/bán. Nhờ vậy logic tính PnL viết một lần, không phải rẽ nhánh theo chuỗi, và bọn em mở rộng được
lên **năm chuỗi (Sui, BSC, Base, Solana, Monad)** mà phần lõi không rối lên. Em làm phần lớn migration đó
**gần như một mình** giai đoạn đầu, trước khi team lớn lên tầm 8–10 người và xây tiếp trên nền đó.

Phần em học được nhiều nhất ở đây là về **tính đúng đắn dưới môi trường phân tán**. Pipeline PnL chạy trên
Kafka, mà Kafka là **at-least-once** — tức là một message có thể được gửi lại nhiều lần. Với tiền thì
'gần đúng' là không chấp nhận được, nên em thiết kế cho nó **idempotent**: dedup trong từng batch, upsert
vào Postgres theo khoá duy nhất, và **commit offset thủ công chỉ sau khi ghi DB thành công**. Em còn
**tính lại PnL từ trạng thái số dư thay vì cộng dồn**, vì như vậy áp cùng một tập sự kiện hai lần vẫn ra
cùng kết quả. Có một chi tiết em khá thích: các partition **không được chia theo user**, nên em xử lý thứ
tự ngay trong consumer — gom theo user rồi áp theo thứ tự thời gian — thay vì bắt Kafka đảm bảo thứ tự.

À, và có một **sự cố production** ở đây mà em hay kể: consumer lag đột nhiên **tồn đọng khoảng 4 triệu
message trong vài tiếng**. Dấu hiệu là lag tăng mà CPU consumer không hề đầy — tức là đang *chờ* chứ không
phải đang *tính*. Root cause là bọn em xử lý **từng-message-một**, mỗi message một lần round-trip DB và một
lần commit, nên có một trần throughput cứng. Em **đổi sang xử lý theo batch**, kéo lag từ hàng triệu xuống
còn tầm vài trăm. Bài học em rút ra là: **chẩn đoán trước khi scale** — thêm consumer không giải quyết được
một chi phí cố định trên mỗi message.

Ngoài RaidenX, em cũng làm ở phía **sàn giao dịch tập trung (VDAX)**, kiểu Binance. Ở đó em xây **engine
hoa hồng giới thiệu (referral)** — nhiều tầng, xử lý tiền chính xác đến từng đồng với kiểu decimal, và chống
trả trùng bằng **ràng buộc duy nhất (user, transaction)** ngay ở tầng database. Em cũng làm **pipeline
thông báo** fan-out ra email, real-time in-app và push, với delivery idempotent theo từng kênh; và đóng góp
vào **service tài khoản** — KYC qua Sumsub, nạp/rút đa chuỗi, và 2FA kiểu TOTP. Điểm chung ở mảng này vẫn là
thứ em quan tâm nhất: **state machine rõ ràng và idempotency** để hệ thống không rơi vào trạng thái sai.

Song song với công việc, em có một **dự án cá nhân là Kotoba Press** — app học tiếng Nhật, em làm để rèn
craft. Backend em viết theo **kiến trúc hexagonal** trong Go, và phần vui nhất là em **tự viết một search
engine bằng C++** dùng thuật toán BM25 thay vì dùng Elasticsearch — vừa để giữ hệ thống gọn nhẹ, vừa vì em
muốn thật sự hiểu search hoạt động bên trong ra sao. Go giao tiếp với nó qua **gRPC**. Em có benchmark
bằng k6, nhưng em cũng thành thật là mấy con số đó là đo một lần, dữ liệu tổng hợp, nên em chỉ tin ở mức độ
lớn thôi.

Còn về việc **vì sao em quan tâm đến công ty mình** — điều em thích nhất trong công việc từ trước tới giờ
là những bài toán mà **tính đúng đắn, độ trễ và quy mô** đều quan trọng cùng lúc, chứ không phải CRUD đơn
thuần. Đó chính xác là loại bài toán mà công ty đang giải ở quy mô lớn hơn nhiều so với những gì em từng
chạm tới, và em muốn được làm cùng những người coi những vấn đề này là chuyện thường ngày, để em học và
lớn lên nhanh hơn. Em nghĩ nền tảng của em về Go, Kafka và hệ thống nhạy về tính đúng đắn khớp khá tốt với
những gì team đang cần.

Đại khái là vậy ạ — em có thể đi sâu vào bất kỳ phần nào anh/chị thấy hứng thú, ví dụ cách em xử lý thứ tự
per-user trên Kafka, hay cái sự cố consumer-lag đó."

---

## Ghi chú khi trình bày (không đọc phần này)

**Dòng thời gian (chronological):** Nghiên cứu Rust/blockchain → chuyển sang backend → RaidenX (mạnh
nhất, nói lâu nhất) → VDAX (mở rộng độ sâu về tiền) → Kotoba Press (craft cá nhân) → vì sao chọn công ty.

**5 "hook" đã cài sẵn — hãy để người phỏng vấn cắn câu:**
1. *"Lớp chuẩn hoá"* để mở rộng đa chuỗi → họ sẽ hỏi về cách trừu tượng hoá chuỗi.
2. *At-least-once + idempotent + tính lại PnL* → họ sẽ hỏi về correctness/dedup/exactly-once.
3. *Partition không chia theo user, xử lý thứ tự trong consumer* → hook về distributed ordering.
4. *Sự cố 4 triệu message, per-message → batch* → hook về debug production và scaling.
5. *Tự viết search engine C++ (BM25) thay vì Elasticsearch* → hook về systems programming/build-vs-buy.

**Điều chỉnh độ dài:**
- Bản ~3 phút: cắt bớt đoạn VDAX và Kotoba xuống mỗi thứ một câu.
- Bản ~5 phút: giữ nguyên, và khi tới sự cố consumer-lag thì kể chậm lại một nhịp.

**Thay `công ty mình` bằng tên công ty thật** và thêm 1 câu cụ thể về sản phẩm/bài toán của họ (ví dụ:
streaming/observability với Datadog, payments với Stripe, exchange với Coinbase/Binance) để đoạn cuối
không bị chung chung.

**Nguyên tắc giọng điệu:** khiêm tốn nhưng tự tin, không buzzword, chủ động thừa nhận giới hạn (số liệu
benchmark, phần làm cùng team) — chính sự thành thật đó là tín hiệu 'senior' mạnh nhất.
