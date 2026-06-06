import { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } from "./config";
import { log } from "./logger";

export async function sendTelegram(message: string): Promise<void> {
  try {
    await fetch(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chat_id: TELEGRAM_CHAT_ID,
          text: message,
          parse_mode: "Markdown",
        }),
      },
    );
  } catch {
    log("Telegram notification failed");
  }
}
