# telcos-to-hubspot-cron

## Qué hace
- Lee **Claro** (`contacts`) con firma HMAC-SHA1.
- **Tigo** es opcional; si no hay credenciales, el job lo salta.
- Envía a **HubSpot** (objeto personalizado) o guarda en **DRY-RUN** en ./data/
- Tiene **cron** diario 08:00 America/Guatemala.

## Uso
1) `cp .env.example .env` (completa CLARO_*; HubSpot si ya lo tienes).
2) `npm install`
3) **Una vez ahora:** `npm start`
4) **Con cron:** `npm run schedule` (deja el proceso corriendo).

## DRY-RUN
- Si `SEND_TO_HUBSPOT=false` o falta token/objeto, se guardan payloads en `./data/*.json`.

## Personaliza
- Mapeos: `src/jobs/sync.js`
- Cron: variables `CRON_EXPRESSION` y `TIMEZONE`
