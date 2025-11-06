// Deno Deploy Cron Job Script (cron_job.ts)

const API_BASE_URL = "http://127.0.0.1:8000"; // (Замени на URL твоего Render-сервера)
const CRON_SECRET = "твой_реальный_cron_secret_из_.env";

// -------------------------------------------------------------------
// Хелпер для отправки запроса и логирования
// -------------------------------------------------------------------
async function postApi(
  endpoint: string,
  body: object | null,
  token: string | null = null
) {
  const url = `${API_BASE_URL}${endpoint}`;
  const headers = new Headers({
    "Content-Type": "application/json",
  });

  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: headers,
      body: body ? JSON.stringify(body) : null,
    });

    // Логируем ответ
    if (response.status === 202) {
      console.log(`[OK] ${endpoint} принят (202 Accepted)`);
    } else if (response.status === 409) {
      console.warn(
        `[WARN] ${endpoint} отклонен (409 Conflict). Воркер уже занят.`
      );
    } else {
      console.error(
        `[FAIL] ${endpoint} вернул ${
          response.status
        }. Ответ: ${await response.text()}`
      );
    }
  } catch (error) {
    console.error(
      `[FAIL] Ошибка сети при вызове ${endpoint}: ${error.message}`
    );
  }
}

// -------------------------------------------------------------------
// Главная функция Deno Cron
// -------------------------------------------------------------------
async function runCronJobs() {
  const now = new Date();
  const hour = now.getUTCHours();
  const minute = now.getUTCMinutes();

  console.log(
    `Cron запущен: ${now.toISOString()} (UTC) | H: ${hour}, M: ${minute}`
  );

  // (Этот скрипт запускается каждую минуту)
  // Твой каскадный график (с 2-минутными окнами):

  // 1. 1h (Высший приоритет)
  if (minute === 1) {
    console.log("Запускаю: 1h");
    await postApi("/get-market-data", { timeframe: "1h" });
  }

  // 2. Funding Rate (FR)
  if (minute === 5 && hour % 4 === 0) {
    // 00:05, 04:05, 08:05...
    console.log("Запускаю: fr");
    await postApi("/api/v1/internal/update-fr", null, CRON_SECRET);
  }

  // 3. 4h / 8h
  if (minute === 8 && hour % 4 === 0) {
    // 00:08, 04:08, 08:08...
    console.log("Запускаю: 4h");
    await postApi("/get-market-data", { timeframe: "4h" });
  }

  // 4. 12h
  if (minute === 11 && (hour === 0 || hour === 12)) {
    // 00:11, 12:11
    console.log("Запускаю: 12h");
    await postApi("/get-market-data", { timeframe: "12h" });
  }

  // 5. 1d
  if (minute === 14 && hour === 0) {
    // 00:14
    console.log("Запускаю: 1d");
    await postApi("/get-market-data", { timeframe: "1d" });
  }
}

// Deno Deploy вызывает 'default'
export default function handler(req: Request) {
  // Запускаем асинхронно, не блокируя ответ
  runCronJobs();

  // Немедленно отвечаем Deno
  return new Response("Cron jobs triggered (async)", { status: 200 });
}
