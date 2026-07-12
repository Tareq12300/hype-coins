# TradingView 4H Category Bot

## الوظائف

- فريم 4 ساعات.
- يعمل على الشمعة المفتوحة.
- يشترط 3 فحوصات متتالية قبل التنبيه.
- تنبيه واحد لكل عملة/منصة في كل شمعة.
- CoinGecko + CoinMarketCap لاكتشاف العملات والفئات.
- TradingView لمؤشرات MACD وStoch RSI وحجم الشمعة.
- يدعم تشغيل وإيقاف الفئات من Environment Variables.
- يحسب متوسط حجم آخر 20 شمعة ونسبة حجم الشمعة المفتوحة.
- يرسل TP1 إلى TP5 ووقف الخسارة.

## التشغيل

```bash
python -m pip install -r requirements.txt
cp .env.example .env
python bot.py
```

في Railway:

```text
Start Command: python bot.py
```

ضع المتغيرات في Railway Variables ولا ترفع ملف `.env` إلى GitHub.

## ملاحظة الشمعة المفتوحة

عندما يكون:

```env
SIGNAL_ON_OPEN_CANDLE=true
REALTIME_CONFIRMATIONS=3
CHECK_INTERVAL=60
```

يجب أن تبقى الشروط متحققة في ثلاثة فحوصات متتالية، أي نحو ثلاث دورات فحص، ثم يُرسل التنبيه مرة واحدة في الشمعة.

## ملاحظة حجم الشمعة

TradingView يعيد حجم العملة الأساسية غالبًا. الكود يحوله إلى قيمة USDT تقريبية:

```text
حجم الشمعة بالـ USDT = volume × close
```

ثم يقارنه بمتوسط آخر `CANDLE_VOLUME_AVG_PERIOD` شمعة.

## ملاحظة أمنية

لا تضع مفاتيح CoinGecko وCoinMarketCap أو Telegram داخل الكود أو GitHub.


## فلتر تغير السعر ودرجة الإشارة

```env
MIN_PRICE_CHANGE_4H=0
MAX_PRICE_CHANGE_4H=20
MIN_SIGNAL_SCORE=80
```

الدرجة من 100 موزعة كالتالي:

- MACD فوق الصفر: 10
- MACD فوق Signal: 10
- MACD صاعد: 10
- Histogram موجب: 10
- Histogram صاعد: 10
- تقاطع Stoch RSI صاعد: 15
- Stoch RSI صاعد: 10
- حجم الشمعة فوق المتوسط: 10
- Volume Ratio يحقق الحد: 10
- تغير السعر ضمن النطاق: 5
