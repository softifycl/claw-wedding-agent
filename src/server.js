// claw-wedding-agent — WhatsApp Wedding Planner Bot
// v1.8.2 - fix PUT minisitio: JSONB arrays (cronograma/faq/timeline/galeria/regalos)
//          se serializan con JSON.stringify antes del bind ::jsonb (09-09-2026)
// v1.8.1 - Minisitio autoadministrable (F1 + fix B1/A2/M3: 2026-08-27)
// Repo canónico: softifycl/claw-wedding-agent
// Mirror (Railway): alejandrochungp/claw-wedding-agent

const express = require('express');
const Redis = require('ioredis');
const axios = require('axios');
const { Pool } = require('pg');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const rateLimit = require('express-rate-limit');

const app = express();
// Railway proxea las peticiones: sin esto req.ip es la IP interna de Railway para TODOS
// y el rate-limit de login colapsa en una sola llave (F1 minisitio, H5 auditoría).
app.set('trust proxy', 1);
app.use(express.json({ type: ['application/json', 'text/plain', 'application/*+json'], verify: (req, _res, buf) => { req.rawBody = buf; } }));

// ── Config ───────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || 'wedding_verify_2026';
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN || '';
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID || '';
const META_APP_ID = process.env.META_APP_ID || '';
const META_APP_SECRET = process.env.META_APP_SECRET || '';
const SLACK_CHANNEL_ID = process.env.SLACK_CHANNEL_ID || '';
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN || '';
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET || '';
const WEDDING_SITE_URL = process.env.WEDDING_SITE_URL || 'https://alejandro-kuilen.noscasamos.vip';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || '';
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || 'deepseek-chat';
const DATABASE_URL = process.env.DATABASE_URL || ''; // Postgres (leads + actores)
const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN || ''; // Mercado Pago (Código Novios)
const CN_SITE_URL = process.env.CN_SITE_URL || 'https://codigonovios.cl';
const LINKIFY_MERCHANT = process.env.LINKIFY_MERCHANT || '4V9PdORjokOya1Y'; // Linkify (Código Novios)
const LINKIFY_PRIVATE_KEY = process.env.LINKIFY_PRIVATE_KEY || ''; // HMAC webhook Linkify
const CN_ADMIN_SECRET = process.env.CN_ADMIN_SECRET || 'cn_admin_secret_2026'; // firma tokens panel novios
// Minisitio autoadministrable (F1): secreto PROPIO, SIN default → fail closed.
// Si Railway no lo setea, el proceso aborta con exit 1 (nunca arranca sin firma).
const MINISITIO_ADMIN_SECRET = process.env.MINISITIO_ADMIN_SECRET;
const CN_SMTP_HOST = process.env.CN_SMTP_HOST || 'mail.aconcaguacapital.cl';
const CN_SMTP_PORT = parseInt(process.env.CN_SMTP_PORT || '465', 10);
const CN_SMTP_USER = process.env.CN_SMTP_USER || 'novios@aconcaguacapital.cl';
const CN_SMTP_PASS = process.env.CN_SMTP_PASS || '';
const CN_SMTP_FROM = process.env.CN_SMTP_FROM || 'Código Novios <novios@aconcaguacapital.cl>';
const CN_SMTP_REPLY_TO = process.env.CN_SMTP_REPLY_TO || 'novios@aconcaguacapital.cl';

const META_API = 'https://graph.facebook.com/v22.0';
const SLACK_API = 'https://slack.com/api';

// ── Postgres (Fase 7: BD de leads, separada de invitados) ───
const pg = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 }) : null;

// Actor registry keys (Fase 1: identificación novio > invitado > lead)
const ACTOR_KEY = 'wedding:actors'; // phone → { role, boda_id, novios, email, createdAt }

// ── Redis ────────────────────────────────────────────────────
const redis = new Redis(REDIS_URL, {
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    if (times > 5) return null;
    return Math.min(times * 200, 2000);
  },
  lazyConnect: true,
});

const RSVP_KEY = 'wedding:rsvps';
const CONVERSATION_KEY = 'wedding:conversations'; // phone → last interaction
const SLACK_TS_KEY = 'wedding:slack_ts'; // wa_msg_id → slack_ts for threading

redis.on('error', (err) => console.error('Redis error:', err.message));

// ── Postgres init (Fase 7: BD de leads separada de invitados) ──
async function initPostgres() {
  if (!pg) {
    console.warn('⚠️ Postgres no configurado (DATABASE_URL vacío)');
    return;
  }
  try {
    await pg.query(`
      CREATE TABLE IF NOT EXISTS leads (
        id SERIAL PRIMARY KEY,
        phone VARCHAR(32) UNIQUE,
        email VARCHAR(255),
        nombres VARCHAR(255),
        fecha_boda DATE,
        ciudad VARCHAR(128),
        n_invitados INT,
        plan_interes VARCHAR(64),
        mensaje TEXT,
        origen VARCHAR(32) DEFAULT 'whatsapp_bot',
        estado VARCHAR(32) DEFAULT 'nuevo',
        notas_marketing TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS cn_novios (
        id SERIAL PRIMARY KEY,
        slug TEXT UNIQUE NOT NULL,
        nombre_novio TEXT,
        nombre_novia TEXT,
        fecha_boda DATE,
        telefono_novio TEXT,
        email TEXT,
        banco TEXT,
        tipo_cuenta TEXT,
        numero_cuenta TEXT,
        titular TEXT,
        rut_titular TEXT,
        password_hash TEXT,
        estado TEXT DEFAULT 'activa',
        activa_hasta DATE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    // Migración idempotente: tabla ya existente sin password_hash
    await pg.query('ALTER TABLE cn_novios ADD COLUMN IF NOT EXISTS password_hash TEXT');
    // Códigos de reseteo de contraseña del panel novios (15 min, un solo uso)
    await pg.query(`
      CREATE TABLE IF NOT EXISTS cn_reset_codigos (
        id SERIAL PRIMARY KEY,
        slug TEXT NOT NULL,
        codigo_hash TEXT NOT NULL,
        expira TIMESTAMPTZ NOT NULL,
        intentos INT DEFAULT 0,
        usado BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pg.query('CREATE INDEX IF NOT EXISTS cn_reset_codigos_slug_idx ON cn_reset_codigos (slug)');
    await pg.query(`
      CREATE TABLE IF NOT EXISTS cn_deseos (
        id SERIAL PRIMARY KEY,
        novio_id INT REFERENCES cn_novios(id) ON DELETE CASCADE,
        nombre TEXT NOT NULL,
        descripcion TEXT,
        foto_url TEXT,
        precio_sugerido INT,
        monto_total INT,
        monto_recaudado INT DEFAULT 0,
        estado TEXT DEFAULT 'activo',
        orden INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pg.query(`
      CREATE TABLE IF NOT EXISTS cn_regalos (
        id SERIAL PRIMARY KEY,
        deseo_id INT REFERENCES cn_deseos(id) ON DELETE SET NULL,
        novio_id INT REFERENCES cn_novios(id) NOT NULL,
        nombre_invitado TEXT,
        mensaje TEXT,
        monto_neto INT NOT NULL,
        comision INT NOT NULL DEFAULT 0,
        monto_total INT NOT NULL,
        mp_preference_id TEXT,
        mp_payment_id TEXT UNIQUE,
        estado TEXT DEFAULT 'pendiente',
        pagado_at TIMESTAMPTZ,
        notificado_invitado BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pg.query('CREATE INDEX IF NOT EXISTS idx_cn_deseos_novio ON cn_deseos(novio_id)');
    await pg.query('CREATE INDEX IF NOT EXISTS idx_cn_regalos_novio ON cn_regalos(novio_id)');
    await pg.query('CREATE INDEX IF NOT EXISTS idx_cn_regalos_estado ON cn_regalos(estado)');
    await pg.query('ALTER TABLE cn_regalos ADD COLUMN IF NOT EXISTS linkify_invoice_id TEXT');
    await pg.query('ALTER TABLE cn_regalos ADD COLUMN IF NOT EXISTS rut_invitado TEXT');
    await pg.query('ALTER TABLE cn_regalos ADD COLUMN IF NOT EXISTS email_invitado TEXT');
    await pg.query('CREATE UNIQUE INDEX IF NOT EXISTS idx_cn_regalos_linkify_invoice ON cn_regalos(linkify_invoice_id)');
    // ── Minisitio nupcial autoadministrable (F1, 27-ago-2026) ────────────────
    // 1 fila por boda. Solo campos PROPIOS del micrositio; identidad/fecha/contacto
    // viven en cn_novios (fuente única). estado = draft/publish (decisión orquestador):
    //   'borrador' → el sitio público responde 404 (no publicado)
    //   'publicada' → visible
    //   'pausada'  → 403 (mismo patrón que /api/codigonovios/lista/:slug)
    await pg.query(`
      CREATE TABLE IF NOT EXISTS cn_minisitio (
        id              SERIAL PRIMARY KEY,
        novio_id        INT UNIQUE NOT NULL REFERENCES cn_novios(id) ON DELETE CASCADE,
        codigo_slug     TEXT UNIQUE NOT NULL REFERENCES cn_novios(slug) ON DELETE CASCADE,
        minisitio_slug  TEXT UNIQUE NOT NULL,
        estado          TEXT NOT NULL DEFAULT 'borrador'
          CHECK (estado IN ('borrador','publicada','pausada')),
        hora            TEXT
          CHECK (hora IS NULL OR hora ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'),
        lugar           TEXT,
        dress_code      TEXT,
        estacionamiento TEXT,
        plus_one        TEXT,
        contacto        TEXT,
        hero_img        TEXT,
        video_url       TEXT,
        cronograma      JSONB NOT NULL DEFAULT '[]'
          CHECK (jsonb_typeof(cronograma) = 'array' AND jsonb_array_length(cronograma) <= 15),
        faq             JSONB NOT NULL DEFAULT '[]'
          CHECK (jsonb_typeof(faq) = 'array' AND jsonb_array_length(faq) <= 10),
        timeline        JSONB NOT NULL DEFAULT '[]'
          CHECK (jsonb_typeof(timeline) = 'array' AND jsonb_array_length(timeline) <= 15),
        galeria         JSONB NOT NULL DEFAULT '[]'
          CHECK (jsonb_typeof(galeria) = 'array' AND jsonb_array_length(galeria) <= 10),
        regalos         JSONB NOT NULL DEFAULT '{}'
          CHECK (jsonb_typeof(regalos) = 'object'),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    // Upgrade idempotente del CHECK de estado (si la tabla ya existía con el CHECK viejo)
    await pg.query('ALTER TABLE cn_minisitio DROP CONSTRAINT IF EXISTS cn_minisitio_estado_check');
    await pg.query("ALTER TABLE cn_minisitio ADD CONSTRAINT cn_minisitio_estado_check CHECK (estado IN ('borrador','publicada','pausada'))");
    // Homepage 13-sep-2026: campos de texto del home (ALTER idempotente).
    //   aviso       → noticia/aviso destacado editable (texto libre corto)
    //   rsvp_plazo  → fecha límite de confirmación (texto libre, ej. "30 de octubre")
    await pg.query('ALTER TABLE cn_minisitio ADD COLUMN IF NOT EXISTS aviso TEXT');
    await pg.query('ALTER TABLE cn_minisitio ADD COLUMN IF NOT EXISTS rsvp_plazo TEXT');
    // Trigger: updated_at automático
    await pg.query(`
      CREATE OR REPLACE FUNCTION cn_set_updated_at() RETURNS trigger AS $$
      BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
      END $$ LANGUAGE plpgsql
    `);
    await pg.query('DROP TRIGGER IF EXISTS trg_cn_minisitio_updated ON cn_minisitio');
    await pg.query(`
      CREATE TRIGGER trg_cn_minisitio_updated
        BEFORE UPDATE ON cn_minisitio
        FOR EACH ROW EXECUTE FUNCTION cn_set_updated_at()
    `);
    // Historial: snapshot del estado ANTERIOR en cada PUT admin (rollback manual <5 min)
    await pg.query(`
      CREATE TABLE IF NOT EXISTS cn_minisitio_historial (
        id                 SERIAL PRIMARY KEY,
        novio_id           INT NOT NULL REFERENCES cn_novios(id) ON DELETE CASCADE,
        minisitio_slug     TEXT NOT NULL,
        campos_anteriores  JSONB NOT NULL,
        campos_nuevos      JSONB,
        autor              TEXT NOT NULL DEFAULT 'novio',
        motivo             TEXT,
        ts                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pg.query('CREATE INDEX IF NOT EXISTS idx_cn_minisitio_historial_novio_ts ON cn_minisitio_historial(novio_id, ts DESC)');
    console.log('✅ Postgres listo — tablas leads + cn_* + cn_minisitio creadas/verificadas');
  } catch (err) {
    console.error('❌ Postgres init error:', err.message);
  }
}

// ── Actor registry (Fase 1: identificación novio > invitado > lead) ──
async function getActorRole(phone) {
  const normalized = normalizePhone(phone);
  // 1. Novios registrados (tenant)
  if (TENANT.noviosPhones.includes(normalized.replace('+', ''))) return 'novio';
  // 2. Actor registrado en Redis (explicito)
  try {
    const raw = await redis.hget(ACTOR_KEY, normalized);
    if (raw) {
      const actor = JSON.parse(raw);
      return actor.role || 'lead';
    }
  } catch (e) { /* ignore */ }
  // 3. Invitado ya confirmado (tiene RSVP)
  try {
    const entries = await redis.lrange(RSVP_KEY, 0, -1);
    for (const e of entries) {
      const r = JSON.parse(e);
      if (r.telefono === normalized) return 'invitado';
    }
  } catch (e) { /* ignore */ }
  // 4. Default: lead (nuevo cliente potencial)
  return 'lead';
}

async function registerActor(phone, role, extra = {}) {
  try {
    const normalized = normalizePhone(phone);
    await redis.hset(ACTOR_KEY, normalized, JSON.stringify({
      role,
      boda_id: TENANT.id,
      createdAt: new Date().toISOString(),
      ...extra,
    }));
    console.log(`👤 Actor registrado: ${normalized} → ${role}`);
  } catch (e) { /* ignore */ }
}

// ── Leads (Fase 7) ────────────────────────────────────────────
async function saveLead(lead) {
  if (!pg) return null;
  try {
    const res = await pg.query(`
      INSERT INTO leads (phone, email, nombres, fecha_boda, ciudad, n_invitados, plan_interes, mensaje, origen)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (phone) DO UPDATE SET
        email = EXCLUDED.email,
        nombres = EXCLUDED.nombres,
        fecha_boda = EXCLUDED.fecha_boda,
        ciudad = EXCLUDED.ciudad,
        n_invitados = EXCLUDED.n_invitados,
        plan_interes = EXCLUDED.plan_interes,
        mensaje = EXCLUDED.mensaje,
        updated_at = NOW()
      RETURNING id
    `, [
      lead.phone || null,
      lead.email || null,
      lead.nombres || null,
      lead.fecha_boda || null,
      lead.ciudad || null,
      lead.n_invitados || null,
      lead.plan_interes || null,
      lead.mensaje || null,
      lead.origen || 'whatsapp_bot',
    ]);
    console.log(`🆕 Lead guardado: ${lead.phone} (id ${res.rows[0]?.id})`);
    return res.rows[0];
  } catch (err) {
    console.error('❌ saveLead error:', err.message);
    return null;
  }
}

async function listLeads() {
  if (!pg) return [];
  try {
    const res = await pg.query('SELECT * FROM leads ORDER BY created_at DESC LIMIT 200');
    return res.rows;
  } catch (err) {
    console.error('❌ listLeads error:', err.message);
    return [];
  }
}

// ── Lead commercial flow (modo lead) ─────────────────────────
async function handleLeadMessage(from, text) {
  console.log(`💼 LEAD [${from}]: ${text.slice(0, 100)}`);
  await saveLead({ phone: from, mensaje: text, origen: 'whatsapp_bot' });

  const lower = text.toLowerCase();
  const reply =
    /precio|cu[aá]nto|costo|plan|tarifa|mensual/i.test(lower)
      ? `¡Hola! 👋 Somos *Nos Casamos* — WhatsApp automático para bodas 💍\n\nNuestros planes:\n• *Esencial* — $49.990 setup + $19.990/mes\n• *Completo* (el más elegido) — $79.990 setup + $29.990/mes\n• *Premium* — $119.990 setup + $39.990/mes\n\nTodo incluye Save the Date, RSVP con botones, recordatorios y micrositio nupcial. ¿Te cuento más en detalle?`
      : `¡Hola! 👋 Somos *Nos Casamos* — el bot de WhatsApp que organiza bodas 💍\n\nInvitados confirman con un toque, reciben recordatorios y dudas respondidas al instante. Tú ves todo en tiempo real.\n\nPara darte una cotización cuéntame: 📅 fecha de la boda, 📍 ciudad y 👥 nº de invitados. O escríbenos a nuestro WhatsApp comercial.`;

  await sendWhatsAppMessage(from, reply);
  await notifySlack(`💼 *LEAD NUEVO* \`${from}\`: "${text.slice(0, 120)}"`);
}

// ── Tenant Config ─────────────────────────────────────────────
const TENANT = {
  id: 'boda-alejandro-kuilen',
  novios: { nombre1: 'Alejandro', nombre2: 'Kuilen' },
  // WhatsApp de los novios (identificables por el bot — Fase 1)
  noviosPhones: ['56966283141', '56956375085'], // Alejandro (+56 9 6628 3141) + Kuilen (+56 9 5637 5085)
  fecha: '2026-11-17',
  hora: '18:00',
  horaNota: 'Sujeto a modificaciones. Por confirmar',
  tipoCelebracion: 'Boda China / Coreana',
  lugar: 'Restaurante Meihua, Av. Pedro Aguirre Cerda 5761, Cerrillos',
  dressCode: 'Semi Formal',
  siteUrl: WEDDING_SITE_URL,
  calendarUrl: 'https://calendar.google.com/calendar/render?action=TEMPLATE&text=Boda+Alejandro+y+Kuilen&dates=20261117T210000Z/20261118T030000Z&details=Boda+de+Alejandro+y+Kuilen+-+Restaurante+Meihua&location=Restaurante+Meihua,+Av.+Pedro+Aguirre+Cerda+5761,+Cerrillos,+Santiago',
  saveTheDateImage: 'https://missclickpro.wordpress.com/wp-content/uploads/2025/07/portadaweb_missclick.jpg',
};

// ── Phone Formatting ─────────────────────────────────────────
// Regex universal: Chile (+56 9... / 56 9... / 9...) e internacional con + obligatorio
// El patrón internacional tolera separadores: +1 786 236-7638, +34 612 345 678, +86 138 0013 8000
const PHONE_RE = /(?:\+?56\s*9\s*\d{4}\s*\d{4}|\+[\d\s\-\(\)]{8,18})/g;
const PHONE_RE_SINGLE = /(?:\+?56\s*9\s*\d{4}\s*\d{4}|\+[\d\s\-\(\)]{8,18})/;

function normalizePhone(phone) {
  // E.164 estricto: +[código país][número] (sin separadores)
  let cleaned = String(phone).replace(/[\s\-\(\)]/g, '');
  if (cleaned.startsWith('+')) return `+${cleaned.slice(1)}`; // preserva código país
  if (cleaned.startsWith('56') && cleaned.length >= 11) return `+${cleaned}`;
  if (cleaned.startsWith('9') && cleaned.length === 9) return `+56${cleaned}`;
  return cleaned;
}

// Quita el prefijo placeholder "invitado" / "invitado:" que quedó en nombres legacy
function cleanName(name) {
  if (!name) return name;
  const cleaned = String(name).replace(/^invitado\s*:?\s*/i, '').trim();
  return cleaned || name;
}

// ── Healthcheck ──────────────────────────────────────────────
app.get('/status', async (_req, res) => {
  let rsvpCount = 0;
  try {
    rsvpCount = await redis.llen(RSVP_KEY);
  } catch (e) { /* redis might not be connected yet */ }
  let pgOk = false;
  if (pg) {
    try { await pg.query('SELECT 1'); pgOk = true; } catch (e) { /* not ready */ }
  }
  res.json({
    status: 'ok',
    name: 'claw-wedding-agent',
    version: '1.8.0',
    uptime: Math.floor(process.uptime()),
    node: process.version,
    tenant: TENANT.id,
    whatsapp: !!WHATSAPP_TOKEN,
    slack: !!SLACK_BOT_TOKEN,
    redis: redis.status === 'ready',
    postgres: pgOk,
    rsvps: rsvpCount,
    phoneNumberId: PHONE_NUMBER_ID ? '***configured***' : 'missing',
    metaApp: META_APP_ID ? `${META_APP_ID.slice(0, 8)}...` : 'missing',
    slackEvents: !!SLACK_SIGNING_SECRET,
    llmRSVP: !!DEEPSEEK_API_KEY,
  });
});

// ── Meta Webhook Verification ────────────────────────────────
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('✅ Webhook verified');
    return res.status(200).send(challenge);
  }
  console.warn('❌ Webhook verification failed');
  return res.sendStatus(403);
});

// ── Meta Webhook Event Handler ───────────────────────────────
app.post('/webhook', async (req, res) => {
  const body = req.body;
  if (body.object !== 'whatsapp_business_account') return res.sendStatus(404);

  for (const entry of body.entry || []) {
    for (const change of entry.changes || []) {
      const value = change.value || {};
      const metadata = value.metadata || {};

      if (PHONE_NUMBER_ID && metadata.phone_number_id !== PHONE_NUMBER_ID) {
        console.log(`⏭️ Skipping msg for ${metadata.phone_number_id} (not ${PHONE_NUMBER_ID})`);
        continue;
      }

      if (value.messages) {
        for (const msg of value.messages) {
          await handleIncomingMessage(msg, metadata.display_phone_number);
        }
      }

      if (value.statuses) {
        for (const status of value.statuses) {
          console.log(`📬 Status [${status.status}]: msg ${status.id} → ${status.recipient_id}`);
          // Forward delivery statuses to Slack
          if (status.status === 'delivered' || status.status === 'read') {
            const emoji = status.status === 'delivered' ? '✅' : '👀';
            await notifySlack(`${emoji} Mensaje *${status.status}* para \`${status.recipient_id}\` (id: ${status.id})`);
          } else if (status.status === 'failed') {
            const errors = (status.errors || []).map(e => `${e.code}: ${e.title}`).join(', ');
            await notifySlack(`⚠️ *FALLO DE ENTREGA* para \`${status.recipient_id}\`: ${errors}`);
          }
        }
      }
    }
  }

  return res.sendStatus(200);
});

// ── Slack Events Endpoint ────────────────────────────────────
// POST /slack/events — receives messages from Slack PE channel
// URL Challenge: Slack sends a verification request on setup
app.post('/slack/events', async (req, res) => {
  const body = req.body;

  // Slack URL Verification (one-time challenge)
  if (body.type === 'url_verification') {
    console.log('✅ Slack URL verification challenge');
    return res.json({ challenge: body.challenge });
  }

  // Acknowledge immediately (Slack requires <3s response)
  res.sendStatus(200);

  // Process events
  if (body.type === 'event_callback') {
    const event = body.event || {};

    // Only process messages from our wedding channel
    if (event.channel !== SLACK_CHANNEL_ID) {
      console.log(`⏭️ Skipping Slack event from channel ${event.channel}`);
      return;
    }

    // Skip messages from bots (including ourselves)
    if (event.bot_id || event.subtype === 'bot_message') return;

    // Only handle user messages
    if (event.type === 'message' && event.user && event.text) {
      await handleSlackMessage(event);
    }
  }
});

// ───────────────────── Slack Message Handler (Slack → WhatsApp) ─────────────────────
async function resolvePhoneFromThread(thread_ts, channel) {
  // 1. Buscar en el mapa phoneToThread (Redis) por thread_ts
  try {
    const keys = await redis.hkeys(CONVERSATION_KEY);
    for (const k of keys) {
      if (k.startsWith('slack:thread:')) {
        const raw = await redis.hget(CONVERSATION_KEY, k);
        if (!raw) continue;
        const data = JSON.parse(raw);
        if (data.thread_ts === thread_ts) return k.replace('slack:thread:', '');
      }
    }
  } catch (e) { /* ignore */ }

  // 2. Fallback: leer el header del thread (primer mensaje) y extraer el teléfono
  try {
    const res = await axios.get(
      `https://slack.com/api/conversations.replies?channel=${channel}&ts=${thread_ts}&limit=1`,
      { headers: { Authorization: `Bearer ${SLACK_BOT_TOKEN}` } }
    );
    const parentMsg = res.data?.messages?.[0];
    const match = parentMsg?.text?.match(/\+?(569\d{8})/);
    if (match) return match[1];
  } catch (e) { /* ignore */ }

  return null;
}

async function handleSlackMessage(event) {
  const { user, text, ts, thread_ts, channel } = event;
  console.log(`💬 Slack [${user}]: ${text.slice(0, 100)}`);

  // Comando: tomar control (estilo Yeppo)
  if (text.trim().toLowerCase() === 'tomar') {
    const phone = await resolvePhoneFromThread(thread_ts, channel);
    if (phone) {
      await notifySlackReply(ts, `🎛️ Tomaste control de \`${phone}\`. Escribe tu respuesta en este thread y llegará al invitado. Escribe \`soltar\` para devolver al bot.`);
    } else {
      await notifySlackReply(ts, '⚠️ No pude identificar el teléfono de este hilo.');
    }
    return;
  }

  // Comando: soltar control
  if (text.trim().toLowerCase() === 'soltar') {
    const phone = await resolvePhoneFromThread(thread_ts, channel);
    if (phone) {
      await notifySlackReply(ts, `✅ Bot retoma la conversación de \`${phone}\`.`);
    }
    return;
  }

  // Formato alternativo: +569XXXXXXXX mensaje (sigue funcionando)
  const phoneMatch = text.match(PHONE_RE_SINGLE);
  let phone = null;
  let message = text;

  if (phoneMatch) {
    phone = normalizePhone(phoneMatch[0]);
    message = text.replace(phoneMatch[0], '').trim();
  } else if (thread_ts) {
    // En thread: resolver el teléfono desde el header (estilo Yeppo)
    phone = await resolvePhoneFromThread(thread_ts, channel);
  }

  if (!phone) {
    await notifySlackReply(ts, '⚠️ No encontré el teléfono. Respondé en el thread del invitado, o escribí: `+569XXXXXXXX tu mensaje`');
    return;
  }

  if (!message) {
    await notifySlackReply(ts, '⚠️ El mensaje está vacío. Escribí tu respuesta en este thread.');
    return;
  }

  // Send the message via WhatsApp
  const result = await sendWhatsAppMessage(phone, message);

  if (result) {
    const preview = message.length > 50 ? message.slice(0, 50) + '...' : message;
    await notifySlackReply(ts, `📤 Enviado a \`${phone}\`: ${preview}`);
  } else {
    await notifySlackReply(ts, `❌ Error al enviar a \`${phone}\`. ¿El número tiene una conversación abierta en las últimas 24h?`);
  }
}

// ── Incoming Message Handler ─────────────────────────────────
async function handleIncomingMessage(msg, fromPhone) {
  const from = msg.from;
  const timestamp = msg.timestamp;
  const msgType = msg.type;

  let text = '';
  let interactiveType = null;
  let interactiveId = null;

  if (msgType === 'text') {
    text = msg.text.body;
  } else if (msgType === 'interactive') {
    const interactive = msg.interactive;
    if (interactive.type === 'button_reply') {
      interactiveType = 'button';
      interactiveId = interactive.button_reply.id;
      text = interactive.button_reply.title;
    } else if (interactive.type === 'list_reply') {
      interactiveType = 'list';
      interactiveId = interactive.list_reply.id;
      text = interactive.list_reply.title;
    }
  } else if (msgType === 'button') {
    // Quick reply de template llega como type=button (no interactive)
    interactiveType = 'button';
    interactiveId = (msg.button && (msg.button.payload || msg.button.text)) || '';
    text = (msg.button && msg.button.text) || '';
  } else if (msgType === 'image') {
    text = '[Imagen recibida]';
  } else {
    text = `[${msgType}]`;
  }

  console.log(`💬 ${from}: ${text}` + (interactiveId ? ` [btn:${interactiveId}]` : ''));

  // ── Fase 1: identificar actor (novio > invitado > lead) ──
  const role = await getActorRole(from);
  console.log(`🎭 Rol detectado para ${from}: ${role}`);

  // Mensajes del form del micrositio (rsvp.html / no-confirmado.html):
  // SIEMPRE se procesan como RSVP, sin importar el rol (un novio también puede probar el form)
  // Tolerante a typos: detecta por estructura (👤 + "Asistencia:") o frase con fuzzy "asist\w*"
  const isFormRsvp = /confirmo mi asist\w* a la boda|no podr\w* asist\w* a la boda|asist\w* a la boda/i.test(text)
    || (/👤/.test(text) && /asist\w*\s*:/i.test(text));

  // Botones SIEMPRE son RSVP (vienen de templates oficiales de la boda)
  if (interactiveType === 'button') {
    await handleButtonReply(from, interactiveId, text);
  } else if (msgType === 'text' && isFormRsvp) {
    // Form del micrositio → parseo estructurado (Fase 2)
    await handleRsvpFormMessage(from, text);
  } else if (msgType === 'text') {
    if (role === 'novio') {
      // Novio: comandos de gestión (Fase 2: agregar invitados, etc.)
      await handleNovioCommand(from, text);
    } else if (role === 'lead' && /precio|cu[aá]nto|costo|plan|tarifa|mensual|cotiz|quiero esto|contratar|producto|nos casamos/i.test(text)) {
      // Lead con intención comercial explícita → flujo comercial (Fase 7)
      await handleLeadMessage(from, text);
    } else {
      // Invitado (o lead sin intención comercial → tratar como invitado)
      await handleTextRSVP(from, text);
      await sendAutoReply(from, text);
      // Si era lead, registrarlo como invitado tras interactuar con la boda
      if (role === 'lead') await registerActor(from, 'invitado', { via: 'texto_boda' });
    }
  }

  // Forward to Slack with thread linking
  if (SLACK_BOT_TOKEN && SLACK_CHANNEL_ID) {
    await sendToSlack(from, text, timestamp, fromPhone, interactiveId, msg.wamid);
  }

  // Store conversation mapping for Slack→WA replies
  try {
    await redis.hset(CONVERSATION_KEY, `wa:${from}`, JSON.stringify({
      lastMessage: text,
      timestamp: new Date(parseInt(timestamp) * 1000).toISOString(),
    }));
    await redis.expire(CONVERSATION_KEY, 86400 * 30);
  } catch (e) { /* ignore */ }
}

// ── RSVP Form Parser (micrositio) ────────────────────────────
// El form de rsvp.html arma un mensaje con emojis por campo:
//   Hola! Confirmo mi asistencia a la boda:
//   👤 Nombre
//   📱 +569...
//   ✅ Asistencia: SÍ 🎉
//   👥 Acompañantes: N
//   🍽 Restricciones: ...
//   🅿️ Estacionamiento: Sí
//   💌 Mensaje: ...
function parseRsvpForm(text) {
  const get = (re) => {
    const m = text.match(re);
    return m ? m[1].trim() : null;
  };
  const parsed = {
    nombre: get(/👤\s*([^\n]+)/),
    phone: get(/📱\s*([^\n]+)/),
    // Labels tolerantes a typos (asistensia/asitencia → asist\w*) y a ñ (Acompañantes/Acompanantes)
    asistencia: get(/asist\w*\s*:\s*([^\n]+)/i),
    acompanantesRaw: get(/acompa[nñ]antes?\s*:\s*([^\n]+)/i),
    restricciones: get(/restric\w*\s*:\s*([^\n]+)/i),
    estacionamiento: get(/estacion\w*\s*:\s*([^\n]+)/i),
    mensaje: get(/💌\s*mensaj\w*\s*:\s*([^\n]+)/i),
  };
  // normalizar acompañantes: solo dígitos, máximo 5 (form permite 0-5; typos tipo 550/05 → primer dígito)
  if (parsed.acompanantesRaw != null) {
    const m = parsed.acompanantesRaw.match(/\d+/);
    if (m) {
      const n = parseInt(m[0], 10);
      parsed.acompanantes = n > 5 ? m[0][0] : String(n);
    } else {
      parsed.acompanantes = '0';
    }
  } else {
    parsed.acompanantes = '0';
  }
  delete parsed.acompanantesRaw;
  return parsed;
}

async function handleRsvpFormMessage(from, text) {
  const d = parseRsvpForm(text);

  // CUPO MÁXIMO (anti-tamper): si el invitado tiene cupo asignado, se respeta como tope.
  // El invitado puede confirmar solo (0) o con hasta su cupo, pero nunca más.
  const g = await getGuest(normalizePhone(from));
  if (g && typeof g.acompanantes === 'number') {
    const cupo = g.acompanantes;
    const n = parseInt(d.acompanantes, 10) || 0;
    d.acompanantes = String(Math.max(0, Math.min(n, cupo)));
    d._cupoEnforced = true;
  }

  const asistRaw = (d.asistencia || '').toLowerCase();

  let status;
  if (/s[ií]|all[iá] estar|🎉/.test(asistRaw)) status = '✅ Confirmado (form)';
  else if (/no|😢/.test(asistRaw)) status = '❌ No asistirá (form)';
  else status = '🤔 Tal vez (form)';

  const entry = {
    timestamp: new Date().toISOString(),
    telefono: from,
    rsvp: status,
    nombre: d.nombre || null,
    acompanantes: d.acompanantes || '0',
    restricciones: d.restricciones || null,
    estacionamiento: d.estacionamiento || null,
    mensaje: d.mensaje || null,
    notas: 'Form micrositio',
    updated: 'nuevo',
  };

  try {
    await redis.rpush(RSVP_KEY, JSON.stringify(entry));
    console.log(`📊 RSVP form guardado: ${from} → ${status}`);
  } catch (err) {
    console.error('❌ RSVP form save error:', err.message);
  }

  // F1: actualizar stage del invitado según su respuesta
  const stageFromStatus = status.includes('Confirmado') ? 'confirmado'
    : status.includes('No asistirá') ? 'no_asistira'
    : 'tal_vez';
  await updateGuestStage(from, stageFromStatus);

  await notifySlack(`🎉 *RSVP FORM* \`${from}\`: ${status}${d.nombre ? ` — ${d.nombre}` : ''}${d.acompanantes && d.acompanantes !== '0' ? ` (${d.acompanantes} acompañantes)` : ''}`);

  // Avisar a los novios (ventana 24h → texto libre; si no hay ventana → plantilla)
  await notifyNoviosRsvp(d, status);

  // Respuesta de confirmación al remitente
  const reply = status.includes('Confirmado')
    ? `¡Gracias por confirmar${d.nombre ? `, ${d.nombre}` : ''}! 🎉 Nos vemos el 17 de noviembre. 📅 Agrega el evento a tu calendario: ${TENANT.calendarUrl}`
    : status.includes('No asistirá')
      ? 'Gracias por avisarnos, lo entendemos completamente 🫶 Te tendremos presente ese día.'
      : '¡Gracias por tu respuesta! 🤔 Si luego decides venir, solo escríbenos "sí, voy".';
  await sendWhatsAppMessage(from, reply);
}

// ── Notificación a novios al recibir RSVP (11-Ago-2026) ────────
async function notifyNoviosRsvp(d, status) {
  const texto = `🎉 Nueva confirmación de asistencia:\n${d.nombre ? `👤 ${d.nombre}\n` : ''}${status}\n👥 Acompañantes: ${d.acompanantes || '0'}${d.mensaje ? `\n💌 ${d.mensaje}` : ''}`;
  for (const phone of TENANT.noviosPhones) {
    try {
      // ventana 24h: ¿el novio interactuó con el bot recientemente?
      let ventana = false;
      try {
        const conv = await redis.hget(CONVERSATION_KEY, `wa:${phone}`);
        if (conv) {
          const c = JSON.parse(conv);
          if (c.timestamp) {
            ventana = (Date.now() - new Date(c.timestamp).getTime()) < 24 * 3600 * 1000;
          }
        }
      } catch (e) { /* sin conversación = sin ventana */ }
      if (ventana) {
        await sendWhatsAppMessage(phone, texto);
        console.log(`📨 RSVP notificado a novio ${phone} (ventana 24h)`);
      } else {
        // sin ventana → plantilla aviso_rsvp_novios_v2 (incluye mensaje del invitado; si no hay mensaje → "—")
        // fallback a v1 (aprobada) si la v2 aún está PENDING
        const msgParam = d.mensaje && d.mensaje.trim() ? d.mensaje.trim() : '—';
        const sendTpl = async (tplName, params) => {
          return axios.post(`${META_API}/${PHONE_NUMBER_ID}/messages`, {
            messaging_product: 'whatsapp',
            to: phone,
            type: 'template',
            template: {
              name: tplName,
              language: { code: 'es' },
              components: [{ type: 'body', parameters: params }],
            },
          }, {
            headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
          });
        };
        try {
          await sendTpl('aviso_rsvp_novios_v2', [
            { type: 'text', text: d.nombre || 'Invitado' },
            { type: 'text', text: status },
            { type: 'text', text: String(d.acompanantes || '0') },
            { type: 'text', text: msgParam },
          ]);
          console.log(`📨 RSVP notificado a novio ${phone} (plantilla aviso_rsvp_novios_v2)`);
        } catch (errV2) {
          // fallback: v1 aprobada (sin mensaje) mientras v2 no esté APPROVED
          await sendTpl('aviso_rsvp_novios', [
            { type: 'text', text: d.nombre || 'Invitado' },
            { type: 'text', text: status },
            { type: 'text', text: String(d.acompanantes || '0') },
          ]);
          console.log(`📨 RSVP notificado a novio ${phone} (fallback plantilla aviso_rsvp_novios)`);
        }
      }
    } catch (err) {
      console.error(`notifyNoviosRsvp → ${phone}:`, err.response?.data?.error?.message || err.message);
    }
  }
}

// ── Novio Commands (Fase 2 + G1/G2 + Parejas) ────────────────
async function handleNovioCommand(from, text) {
  const lower = text.trim().toLowerCase();
  console.log(`🎛️ Comando novio [${from}]: ${text.slice(0, 100)}`);

  // G1: confirmación de eliminación pendiente ("sí, eliminar" / "confirmar")
  if (/s[ií],\s*eliminar|confirmar eliminaci[oó]n|s[ií]\s*eliminar/i.test(lower)) {
    try {
      const pending = await redis.get(`wedding:pend_delete:${from}`);
      if (pending) {
        await redis.del(`wedding:pend_delete:${from}`);
        const guest = await deleteGuest(pending);
        await sendWhatsAppMessage(from, `✅ *${guest?.name || pending}* (${pending}) eliminado de los invitados${guest?.stage ? ` (stage: ${guest.stage})` : ''} — incluido su RSVP.`);
        await notifySlack(`🗑️ *Invitado eliminado* por novio \`${from}\`: ${guest?.name || pending} (${pending}) — Opción B (RSVP borrado)`);
      } else {
        await sendWhatsAppMessage(from, 'ℹ️ No hay ninguna eliminación pendiente.');
      }
    } catch (e) {
      console.error('❌ confirmar eliminación error:', e.message);
      await sendWhatsAppMessage(from, '⚠️ Error al eliminar. Inténtalo de nuevo.');
    }
    return;
  }

  // G1: eliminar invitado (con confirmación)
  if (/eliminar invitado|quitar a|borrar invitado/i.test(lower) && PHONE_RE_SINGLE.test(text)) {
    const phoneMatch = text.match(PHONE_RE_SINGLE);
    const phone = normalizePhone(phoneMatch[0]);
    const guest = await getGuest(phone);
    if (!guest) {
      await sendWhatsAppMessage(from, `⚠️ *${phone}* no está en la lista de invitados.`);
      return;
    }
    await redis.set(`wedding:pend_delete:${from}`, phone, 'EX', 120); // TTL 2 min
    await sendWhatsAppMessage(from, `⚠️ ¿Eliminar a *${guest.name}* (${phone})?\n\nStage: ${guest.stage}\n📨 Templates enviados: ${(guest.templatesSent || []).length}\n\n➡️ Escribe *"sí, eliminar"* para confirmar (se borrará también su RSVP).`);
    return;
  }

  // G2: ver invitados (listado completo con stages)
  if (/ver invitados|lista invitados|listado de invitados/i.test(lower)) {
    try {
      const all = await redis.hgetall('wedding:guests');
      const guests = Object.entries(all).map(([phone, raw]) => ({ phone, ...JSON.parse(raw) }));
      const stages = {};
      for (const g of guests) stages[g.stage || 'sin_stage'] = (stages[g.stage || 'sin_stage'] || 0) + 1;
      let msg = `📋 *Invitados (${guests.length}):*\n`;
      msg += `🆕 nuevo: ${stages.nuevo || 0} · 📨 invitación: ${stages.invitacion_enviada || 0} · ✅ confirmados: ${stages.confirmado || 0} · ❌ no: ${stages.no_asistira || 0} · 🤔 talvez: ${stages.tal_vez || 0}\n\n`;
      const emoji = { nuevo: '🆕', invitacion_enviada: '📨', confirmado: '✅', no_asistira: '❌', tal_vez: '🤔' };
      for (const g of guests.slice(0, 20)) {
        msg += `${emoji[g.stage] || '❔'} ${g.name} — ${g.phone}${typeof g.acompanantes === 'number' ? ` · cupo ${g.acompanantes}` : ''}${g.coupleId ? ' 👫' : ''}\n`;
      }
      if (guests.length > 20) msg += `\n... y ${guests.length - 20} más`;
      await sendWhatsAppMessage(from, msg);
    } catch (e) {
      console.error('❌ ver invitados error:', e.message);
      await sendWhatsAppMessage(from, '⚠️ No pude listar los invitados.');
    }
    return;
  }

  // Parejas: vincular 2 invitados existentes
  if (/vincular pareja|vincular a|unir pareja/i.test(lower) && (text.match(PHONE_RE) || []).length >= 2) {
    const phones = (text.match(PHONE_RE) || []).map(p => normalizePhone(p));
    const res = await linkCouple(phones[0], phones[1]);
    if (res.ok) {
      await sendWhatsAppMessage(from, `👫 *Pareja vinculada:*\n• ${res.g1.name} (${phones[0]})\n• ${res.g2.name} (${phones[1]})\n🔗 ${res.coupleId}\n\nEl +1 mutuo se contará una sola vez.`);
      await notifySlack(`👫 *Pareja vinculada* por novio: ${res.g1.name} ↔ ${res.g2.name}`);
    } else {
      const missing = res.reason === 'ambos' ? 'ninguno de los dos' : res.reason;
      await sendWhatsAppMessage(from, `⚠️ No pude vincular: ${missing} no está en la lista de invitados.`);
    }
    return;
  }

  if (/agregar|a[nñ]ade?|agrega|nuevo invitado|invitado/i.test(lower) && PHONE_RE_SINGLE.test(text)) {
    // Fase 2: parser extrae nombre + WhatsApp (+ correo opcional) — soporta parejas
    await addGuestViaChat(from, text);
    return;
  }

  // F1: enviar invitación a un invitado específico
  if (/enviar invitaci[oó]n a|invitar a/i.test(lower) && PHONE_RE_SINGLE.test(text)) {
    const phoneMatch = text.match(PHONE_RE_SINGLE);
    await sendInviteToGuest(from, normalizePhone(phoneMatch[0]));
    return;
  }

  // G3: reenviar invitación (sin dedupe)
  if (/reenviar invitaci[oó]n a|reenviar a/i.test(lower) && PHONE_RE_SINGLE.test(text)) {
    const phoneMatch = text.match(PHONE_RE_SINGLE);
    await sendInviteToGuest(from, normalizePhone(phoneMatch[0]), { force: true });
    return;
  }

  // G4: editar invitado (correo / nombre / teléfono)
  // editar correo de {phone} a {email}
  if (/editar correo de/i.test(lower) && PHONE_RE_SINGLE.test(text) && /[\w.+-]+@[\w-]+\.[\w.]+/.test(text)) {
    const phoneMatch = text.match(PHONE_RE_SINGLE);
    const emailMatch = text.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
    const phone = normalizePhone(phoneMatch[0]);
    const res = await editGuest(phone, 'email', emailMatch[0]);
    if (res.ok) await sendWhatsAppMessage(from, `✅ Correo de *${res.guest.name}* actualizado: ${res.changed.from} → ${res.changed.to}`);
    else await sendWhatsAppMessage(from, `⚠️ No pude editar: ${res.reason === 'no_existe' ? 'el invitado no existe' : 'error'}`);
    return;
  }

  // editar nombre de {phone} a {nombre}
  if (/editar nombre de/i.test(lower) && PHONE_RE_SINGLE.test(text)) {
    const phoneMatch = text.match(PHONE_RE_SINGLE);
    const phone = normalizePhone(phoneMatch[0]);
    // Extraer nombre después de "a " (regex exacta, evita split por 'a ' que rompe con 'María')
    const nameMatch = text.match(/a\s+([^\n]+)$/);
    const name = nameMatch ? nameMatch[1].trim() : null;
    if (!name) {
      await sendWhatsAppMessage(from, `⚠️ Formato: *"editar nombre de {phone} a {Nombre}"*`);
      return;
    }
    const res = await editGuest(phone, 'name', name);
    if (res.ok) await sendWhatsAppMessage(from, `✅ Nombre de *${res.changed.from}* actualizado a *${res.changed.to}* (${phone}).`);
    else await sendWhatsAppMessage(from, `⚠️ No pude editar: ${res.reason === 'no_existe' ? 'el invitado no existe' : 'error'}`);
    return;
  }

  // editar acompañantes (cupo) de {phone} a {n}
  if (/editar (acompa[nñ]antes|cupo) de/i.test(lower) && PHONE_RE_SINGLE.test(text)) {
    const phoneMatch = text.match(PHONE_RE_SINGLE);
    const phone = normalizePhone(phoneMatch[0]);
    const cupoMatch = text.match(/(?:a\s+)?(\d+)\s*$/);
    if (!cupoMatch) {
      await sendWhatsAppMessage(from, `⚠️ Formato: *"editar acompañantes de {phone} a {n}"* (0-5)`);
      return;
    }
    const n = parseInt(cupoMatch[1], 10);
    const res = await editGuest(phone, 'acompanantes', n);
    if (res.ok) await sendWhatsAppMessage(from, `✅ Cupo de *${res.guest.name}* actualizado: ${res.changed.from ?? 'sin cupo'} → *${res.changed.to}* acompañantes.`);
    else await sendWhatsAppMessage(from, `⚠️ No pude editar: ${res.reason === 'no_existe' ? 'el invitado no existe' : 'error'}`);
    return;
  }

  // editar teléfono de {viejo} a {nuevo} — reemplaza con aviso
  if (/editar tel[eé]fono de/i.test(lower) && (text.match(PHONE_RE) || []).length >= 2) {
    const phones = (text.match(PHONE_RE) || []).map(p => normalizePhone(p));
    const res = await editGuest(phones[0], 'phone', phones[1]);
    if (res.ok) {
      await sendWhatsAppMessage(from, `✅ Teléfono de *${res.guest.name}* actualizado: ${res.changed.from} → ${res.changed.to}\n⚠️ El teléfono anterior fue *reemplazado* (ya no es válido).`);
      await notifySlack(`✏️ *Teléfono actualizado* por novio: ${res.guest.name} ${res.changed.from} → ${res.changed.to}`);
    } else {
      const msg = { no_existe: 'el invitado no existe', phone_en_uso: 'el nuevo teléfono ya está en la lista', mismo_phone: 'el teléfono es el mismo' }[res.reason] || 'error';
      await sendWhatsAppMessage(from, `⚠️ No pude editar el teléfono: ${msg}.`);
    }
    return;
  }

  // F1: batch a todos los pendientes
  if (/enviar invitaci[oó]n a todos|invitar a todos|enviar a todos los pendientes/i.test(lower)) {
    await sendInviteToAll(from);
    return;
  }

  if (/ver (los |las )?(confirmaciones|invitados)|cu[aá]ntos confirm|estado/i.test(lower)) {
    try {
      const s = await getConfirmedStats(); // con absorción de parejas 👫
      await sendWhatsAppMessage(from, `📊 *Estado de confirmaciones:*\n✅ Confirmados: ${s.confirmed}\n❌ No asistirán: ${s.declined}\n🤔 Tal vez: ${s.maybe}\n👥 Asistentes estimados: ${s.totalAsistentes}\n\n(Total registrados: ${s.totalRegistrados})\n\n📋 ¿Quieres ver *los nombres* de los confirmados?\n➡️ Responde: *"ver nombres"*`);
    } catch (e) {
      await sendWhatsAppMessage(from, '⚠️ No pude consultar las confirmaciones ahora.');
    }
    return;
  }

  // Segunda opción: listar los NOMBRES de los confirmados (y no-asistentes)
  if (/ver nombres|nombres de los confirmados|qui[eé]nes (son|van|confirman)|listado/i.test(lower)) {
    try {
      const entries = await redis.lrange(RSVP_KEY, 0, -1);
      const rsvps = entries.map(e => JSON.parse(e));

      const confirmed = rsvps.filter(r => r.rsvp.includes('Confirmado'));
      const declined = rsvps.filter(r => r.rsvp.includes('No asistir'));
      const maybe = rsvps.filter(r => r.rsvp.includes('Tal vez'));

      const fmt = (r) => {
        const nombre = r.nombre && r.nombre !== 'null' ? r.nombre : r.telefono;
        const acomp = r.acompanantes && r.acompanantes !== '0' ? ` (+${r.acompanantes})` : '';
        return `• ${nombre}${acomp}`;
      };

      let msg = `📋 *Listado de confirmaciones:*\n\n`;
      msg += `✅ *Confirmados (${confirmed.length}):*\n${confirmed.length ? confirmed.map(fmt).join('\n') : '—'}\n\n`;
      if (maybe.length) msg += `🤔 *Tal vez (${maybe.length}):*\n${maybe.map(fmt).join('\n')}\n\n`;
      msg += `❌ *No asistirán (${declined.length}):*\n${declined.length ? declined.map(fmt).join('\n') : '—'}`;

      await sendWhatsAppMessage(from, msg);
    } catch (e) {
      console.error('❌ ver nombres error:', e.message);
      await sendWhatsAppMessage(from, '⚠️ No pude obtener el listado ahora.');
    }
    return;
  }

  // Comando no reconocido — menú rápido
  await sendWhatsAppMessage(from, `🎛️ *Panel de novios* — comandos disponibles:\n\n➕ *"agregar a {nombre} +56 9..."* — añadir invitado (o pareja: *"agregar a A +56 9... y B +56 9..."*; con cupo: *"... cupo 2"*)\n📨 *"enviar invitación a {phone}"* — enviar save-the-date a uno\n📨 *"reenviar invitación a {phone}"* — reenviar sin dedupe\n📨 *"enviar invitación a todos"* — batch a pendientes\n📋 *"ver invitados"* — listado con stages y cupo\n📊 *"ver confirmaciones"* — estado RSVP\n👥 *"editar acompañantes de {phone} a {n}"* — fijar cupo (0-5)\n👫 *"vincular pareja {p1} {p2}"* — vincular 2 invitados (fix +1)\n✏️ *"editar correo/nombre/teléfono de {phone} a ..."* — editar invitado\n🗑️ *"eliminar invitado {phone}"* — eliminar (con confirmación)\n\n¿Qué necesitas?`);
}

// ── Cupo parser ──────────────────────────────────────────────
// "cupo 3" / "con 3 acompañantes" / "3 acompañantes" → 3 (clamp 0..5)
function parseCupo(text) {
  const m = text.match(/cupo\s*(\d+)/i) || text.match(/(?:con\s+)?(\d+)\s*acompa[nñ]antes?/i);
  if (m) return Math.max(0, Math.min(5, parseInt(m[1], 10)));
  return null;
}

// ── Fase 2: agregar invitado conversacional (soporta PAREJAS 👫) ──
async function addGuestViaChat(from, text) {
  // Parsear: "agrega a María Pérez +56 9 1234 5678 maria@gmail.com"
  // Pareja: "agrega a María Pérez +56 9 1111 2222 y Juan Soto +56 9 3333 4444"
  const phonesRaw = text.match(PHONE_RE) || [];
  const cupo = parseCupo(text); // cupo fijo opcional (acompañantes permitidos)
  if (!phonesRaw.length) {
    await sendWhatsAppMessage(from, `⚠️ No encontré el WhatsApp del invitado. Formato:\n\n➡️ *"agregar a María Pérez +56 9 1234 5678"*\n\nPareja: *"agregar a María +56 9... y Juan +56 9..."* (correo opcional)`);
    return;
  }

  const emailMatch = text.match(/[\w.+-]+@[\w-]+\.[\w.]+/);
  const email = emailMatch ? emailMatch[0] : null;

  const isCouple = phonesRaw.length >= 2;
  if (isCouple) {
    // ── PAREJA: crear 2 invitados vinculados (coupleId + partnerPhone) ──
    const phone1 = normalizePhone(phonesRaw[0]);
    const phone2 = normalizePhone(phonesRaw[1]);

    // Bloqueo de duplicados: verificar AMBOS teléfonos antes de crear
    const dup1 = await getGuest(phone1);
    const dup2 = await getGuest(phone2);
    const dups = [];
    if (dup1) dups.push(`• ${dup1.name} (${phone1}) — stage: ${dup1.stage} · envíos: ${(dup1.templatesSent || []).length} · agregado: ${(dup1.createdAt || '').slice(0, 10)}`);
    if (dup2) dups.push(`• ${dup2.name} (${phone2}) — stage: ${dup2.stage} · envíos: ${(dup2.templatesSent || []).length} · agregado: ${(dup2.createdAt || '').slice(0, 10)}`);
    if (dups.length) {
      await sendWhatsAppMessage(from, `⚠️ No agregué la pareja — ya hay invitados con ese teléfono:\n\n${dups.join('\n')}\n\n➡️ Revisa con *"ver invitados"* antes de decidir.`);
      return;
    }

    const coupleId = 'CP-' + Math.random().toString(16).slice(2, 8).toUpperCase();

    // Dividir el texto en las dos mitades usando los teléfonos como ancla
    const idx1 = text.indexOf(phonesRaw[0]);
    const idx2 = text.indexOf(phonesRaw[1]);
    let name1 = text.slice(0, idx1).replace(/[\w.+-]+@[\w-]+\.[\w.]+/g, '').trim();
    let name2 = text.slice(idx1 + phonesRaw[0].length, idx2).replace(/\s+y\s+/i, ' ').trim();
    name1 = name1.replace(/^(agregar|agrega|a[nñ]ade|a|nuevo invitado|invitado)\s+/i, '').replace(/^(a|al|la|el)\s+/i, '').trim();
    name2 = name2.replace(/^(y|a|al|la|el)\s+/i, '').replace(/^(a|al|la|el)\s+/i, '').trim();
    const finalName1 = name1 || phone1;
    const finalName2 = name2 || phone2;

    const g1 = { name: finalName1, phone: phone1, email: null, addedBy: from, createdAt: new Date().toISOString(), stage: 'nuevo', stageUpdatedAt: new Date().toISOString(), templatesSent: [], coupleId, partnerPhone: phone2 };
    const g2 = { name: finalName2, phone: phone2, email: null, addedBy: from, createdAt: new Date().toISOString(), stage: 'nuevo', stageUpdatedAt: new Date().toISOString(), templatesSent: [], coupleId, partnerPhone: phone1 };
    await redis.hset('wedding:guests', phone1, JSON.stringify(g1));
    await redis.hset('wedding:guests', phone2, JSON.stringify(g2));
    await registerActor(phone1, 'invitado', { name: finalName1 });
    await registerActor(phone2, 'invitado', { name: finalName2 });
    await notifySlack(`👫 *Pareja agregada por novio* \`${from}\`:\n👤 ${finalName1} (${phone1})\n👤 ${finalName2} (${phone2})\n🔗 ${coupleId}`);
    await sendWhatsAppMessage(from, `✅ Agregué a la *pareja* 👫\n👤 ${finalName1} (${phone1})\n👤 ${finalName2} (${phone2})\n\nCada uno recibirá su invitación personalizada y el +1 mutuo se contará una sola vez.\n➡️ Envía invitación: *"enviar invitación a todos"*`);
    return;
  }

  // ── INVITADO INDIVIDUAL ──
  const phone = normalizePhone(phonesRaw[0]);

  // Bloqueo de duplicados: si el teléfono ya existe, NO sobrescribir
  const existing = await getGuest(phone);
  if (existing) {
    await sendWhatsAppMessage(from, `⚠️ *${existing.name}* (${phone}) ya está en la lista — no lo agregué de nuevo.\n\n• Stage: ${existing.stage}\n• Invitación: ${(existing.templatesSent || []).length} envío(s)\n• Agregado: ${(existing.createdAt || '').slice(0, 10)}\n\n➡️ Revisa con *"ver invitados"* antes de decidir.`);
    return;
  }

  // Nombre = texto entre "agregar a" y el teléfono (o correo)
  let namePart = text.replace(phonesRaw[0], '').replace(/\s*\S+@\S+\s*/, ' ').trim();
  namePart = namePart.replace(/^(agregar|agrega|a[nñ]ade|a|nuevo invitado|invitado)\s+/i, '').trim();
  namePart = namePart.replace(/^(a|al|la|el)\s+/i, '').trim();
  const name = namePart || phone;

  // Guardar invitado en Redis (hash: phone → guest JSON, con ciclo de vida F1)
  try {
    const guestKey = 'wedding:guests';
    const guest = {
      name, phone, email, addedBy: from, createdAt: new Date().toISOString(),
      stage: 'nuevo',                                    // ← F1: ciclo de vida
      stageUpdatedAt: new Date().toISOString(),
      templatesSent: [],                                 // ← F1: historial de envíos
    };
    if (cupo != null) guest.acompanantes = cupo;         // ← cupo fijo (acompañantes permitidos)
    await redis.hset(guestKey, phone, JSON.stringify(guest));
    // Registrar actor como invitado
    await registerActor(phone, 'invitado', { name, email });
    await notifySlack(`➕ *Invitado agregado por novio* \`${from}\`:\n👤 ${name}\n📱 ${phone}${email ? `\n📧 ${email}` : ''}${cupo != null ? `\n👥 Cupo: ${cupo}` : ''}`);
    await sendWhatsAppMessage(from, `✅ Agregué a *${name}* (${phone})${email ? `, correo ${email}` : ''} a los invitados (stage: nuevo)${cupo != null ? ` con cupo de *${cupo}* acompañantes` : ''}.\n\n➡️ Cuando quieras, envía la invitación con: *"enviar invitación a ${phone}"*`);
  } catch (e) {
    console.error('❌ addGuestViaChat error:', e.message);
    await sendWhatsAppMessage(from, '⚠️ No pude guardar el invitado. Inténtalo de nuevo.');
  }
}

// ── F1: helpers ciclo de vida de invitados ──────────────────
async function getGuest(phone) {
  try {
    const raw = await redis.hget('wedding:guests', phone);
    return raw ? JSON.parse(raw) : null;
  } catch (e) { return null; }
}

async function updateGuestStage(phone, stage) {
  const guest = await getGuest(phone);
  if (!guest) return;
  guest.stage = stage;
  guest.stageUpdatedAt = new Date().toISOString();
  await redis.hset('wedding:guests', phone, JSON.stringify(guest));
  console.log(`🎯 Stage ${phone}: → ${stage}`);
  return guest;
}

async function recordTemplateSent(phone, templateName, wamid) {
  const guest = await getGuest(phone);
  if (!guest) return;
  guest.templatesSent = guest.templatesSent || [];
  guest.templatesSent.push({ name: templateName, ts: new Date().toISOString(), wamid: wamid || null });
  await redis.hset('wedding:guests', phone, JSON.stringify(guest));
}

// ── G1: eliminar invitado (Opción B: borra también su RSVP) ──
async function deleteGuest(phone) {
  const guest = await getGuest(phone);
  // 1. Borrar del hash de guests
  await redis.hdel('wedding:guests', phone);
  // 2. Borrar actor (si existe)
  try { await redis.hdel(ACTOR_KEY, phone); } catch (e) { /* ignore */ }
  // 3. Opción B: borrar RSVP asociado de la lista wedding:rsvps
  try {
    const entries = await redis.lrange(RSVP_KEY, 0, -1);
    const remaining = entries.filter(raw => {
      try { return JSON.parse(raw).telefono !== phone; } catch (e) { return true; }
    });
    if (remaining.length !== entries.length) {
      await redis.del(RSVP_KEY);
      if (remaining.length) await redis.rpush(RSVP_KEY, ...remaining);
    }
  } catch (e) { /* ignore */ }
  console.log(`🗑️ Invitado eliminado (Opción B): ${phone}`);
  return guest;
}

// ── Parejas 👫: vincular 2 invitados existentes ──
async function linkCouple(phone1, phone2) {
  const g1 = await getGuest(phone1);
  const g2 = await getGuest(phone2);
  if (!g1 || !g2) return { ok: false, reason: !g1 && !g2 ? 'ambos' : (!g1 ? phone1 : phone2) };
  const coupleId = 'CP-' + Math.random().toString(16).slice(2, 8).toUpperCase();
  g1.coupleId = coupleId; g1.partnerPhone = phone2;
  g2.coupleId = coupleId; g2.partnerPhone = phone1;
  await redis.hset('wedding:guests', phone1, JSON.stringify(g1));
  await redis.hset('wedding:guests', phone2, JSON.stringify(g2));
  return { ok: true, coupleId, g1, g2 };
}

// ── G4: editar invitado (correo / nombre / teléfono) ──
async function editGuest(phone, field, value) {
  const guest = await getGuest(phone);
  if (!guest) return { ok: false, reason: 'no_existe' };

  if (field === 'phone') {
    // Reemplazar teléfono: mover registro + actualizar pareja + actor
    const newPhone = normalizePhone(value);
    if (newPhone === phone) return { ok: false, reason: 'mismo_phone' };
    if (await getGuest(newPhone)) return { ok: false, reason: 'phone_en_uso' };
    await redis.hdel('wedding:guests', phone);
    guest.phone = newPhone;
    await redis.hset('wedding:guests', newPhone, JSON.stringify(guest));
    // Actualizar partnerPhone del otro miembro de la pareja
    if (guest.coupleId && guest.partnerPhone) {
      const partner = await getGuest(guest.partnerPhone);
      if (partner) {
        partner.partnerPhone = newPhone;
        await redis.hset('wedding:guests', partner.phone, JSON.stringify(partner));
      }
    }
    // Re-registrar actor
    try { await redis.hdel(ACTOR_KEY, phone); } catch (e) { /* ignore */ }
    await registerActor(newPhone, 'invitado', { name: guest.name, email: guest.email });
    console.log(`✏️ Teléfono actualizado: ${phone} → ${newPhone} (${guest.name})`);
    return { ok: true, guest, changed: { from: phone, to: newPhone } };
  }

  if (field === 'name' || field === 'email' || field === 'acompanantes') {
    const old = guest[field];
    guest[field] = field === 'acompanantes' ? Math.max(0, Math.min(5, parseInt(value, 10) || 0)) : value;
    await redis.hset('wedding:guests', phone, JSON.stringify(guest));
    console.log(`✏️ ${field} actualizado para ${phone}: ${old} → ${guest[field]}`);
    return { ok: true, guest, changed: { field, from: old, to: guest[field] } };
  }

  return { ok: false, reason: 'campo_invalido' };
}

// ── Stats con absorción de parejas (fix +1 duplicado) ──
async function getConfirmedStats() {
  const entries = await redis.lrange(RSVP_KEY, 0, -1);
  const rsvps = entries.map(e => JSON.parse(e));
  const guestsAll = await redis.hgetall('wedding:guests');
  const guestMap = {};
  for (const [p, raw] of Object.entries(guestsAll || {})) {
    try { guestMap[p] = JSON.parse(raw); } catch (e) { /* ignore */ }
  }

  let confirmed = 0, declined = 0, maybe = 0, totalAsistentes = 0;
  const confirmedPhones = new Set(rsvps.filter(r => r.rsvp.includes('Confirmado')).map(r => r.telefono));

  for (const r of rsvps) {
    if (r.rsvp.includes('Confirmado')) {
      confirmed++;
      totalAsistentes++; // el invitado mismo
      const acomp = parseInt(r.acompanantes || '0', 10) || 0;
      let extra = acomp;
      const guest = guestMap[r.telefono];
      // Si tiene pareja vinculada y la pareja TAMBIÉN confirmó → el +1 mutuo se absorbe
      if (guest && guest.coupleId && guest.partnerPhone && confirmedPhones.has(guest.partnerPhone)) {
        extra = Math.max(0, acomp - 1);
      }
      totalAsistentes += extra;
    } else if (r.rsvp.includes('No asistir')) {
      declined++;
    } else if (r.rsvp.includes('Tal vez')) {
      maybe++;
    }
  }
  return { confirmed, declined, maybe, totalAsistentes, totalRegistrados: rsvps.length };
}

// F1: migrar wedding:guests de LISTA (estructura vieja) a HASH (nueva)
async function migrateGuestsToListHash() {
  try {
    const type = await redis.type('wedding:guests');
    if (type !== 'list') return; // ya es hash o no existe
    console.log('🔄 Migrando wedding:guests de lista → hash...');
    const entries = await redis.lrange('wedding:guests', 0, -1);
    const migrados = [];
    for (const raw of entries) {
      try {
        const g = JSON.parse(raw);
        if (!g.phone) continue;
        g.stage = g.stage || 'nuevo';
        g.stageUpdatedAt = g.stageUpdatedAt || g.createdAt || new Date().toISOString();
        g.templatesSent = g.templatesSent || [];
        migrados.push(g);
      } catch (e) { /* entry corrupta, saltar */ }
    }
    // hset falla si la key sigue siendo LIST → borrar la lista primero
    await redis.del('wedding:guests');
    for (const g of migrados) {
      await redis.hset('wedding:guests', g.phone, JSON.stringify(g));
    }
    console.log(`✅ Migración lista→hash: ${migrados.length} invitados`);
  } catch (e) {
    console.error('❌ migrateGuestsToListHash error:', e.message);
  }
}

// URL de la foto para templates con IMAGE header (micrositio, verificado 200)
const SAVE_THE_DATE_IMG_URL = (TENANT.siteUrl || 'https://alejandro-kuilen.noscasamos.vip').replace(/\/$/, '') + '/assets/foto-pareja.jpg';

// Template activo para invitación. v5_img agrega 2 botones URL dinámicos (?phone={{1}} por botón,
// variable independiente por componente). Activar vía env SAVE_THE_DATE_TEMPLATE=save_the_date_v5_img
// cuando Meta apruebe (id 1707548530350870, PENDING). Hasta entonces cae a v4_img (botones estáticos).
const SAVE_THE_DATE_TEMPLATE = process.env.SAVE_THE_DATE_TEMPLATE || 'save_the_date_v4_img';

// Enviar invitación (save_the_date_v4_img con header IMAGE) a un invitado
async function sendInviteToGuest(from, phone, opts = {}) {
  const guest = await getGuest(phone);
  if (!guest) {
    await sendWhatsAppMessage(from, `⚠️ *${phone}* no está en la lista de invitados.\n\n➡️ Primero agrégalo: *"agregar a Nombre ${phone}"*`);
    return;
  }
  // Dedupe: si ya tiene invitación enviada y no está en 'nuevo', avisar (salvo force/reenvío)
  const already = (guest.templatesSent || []).some(t => t.name === SAVE_THE_DATE_TEMPLATE);
  if (already && guest.stage !== 'nuevo' && !opts.force) {
    await sendWhatsAppMessage(from, `ℹ️ A *${guest.name}* ya se le envió la invitación (stage: ${guest.stage}).\n\n➡️ Si quieres reenviarla igual: *"reenviar invitación a ${phone}"*`);
    return;
  }
  try {
    const mediaId = await uploadImageToMeta(SAVE_THE_DATE_IMG_URL);
    const result = await sendInviteTemplate(phone, mediaId);
    if (result?.messages?.[0]?.id) {
      await recordTemplateSent(phone, SAVE_THE_DATE_TEMPLATE, result.messages[0].id);
      await updateGuestStage(phone, 'invitacion_enviada');
      const totalEnvio = (guest.templatesSent || []).length + 1;
      await sendWhatsAppMessage(from, opts.force
        ? `✅ *Reenvío* enviado a *${guest.name}* (${phone}).\n📨 Envíos totales: ${totalEnvio}`
        : `✅ Invitación enviada a *${guest.name}* (${phone}).\nStage: invitacion_enviada`);
      await notifySlack(opts.force
        ? `📨 *Reenvío* a \`${guest.name}\` (${phone}) — envío #${totalEnvio}`
        : `📨 *Invitación enviada* a \`${guest.name}\` (${phone}) por comando del novio`);
    } else {
      await sendWhatsAppMessage(from, `❌ No se pudo enviar la invitación a ${phone}. Verifica el template/estado del número.`);
    }
  } catch (e) {
    console.error('❌ sendInviteToGuest error:', e.message);
    await sendWhatsAppMessage(from, `❌ Error al enviar invitación a ${phone}: ${e.message.slice(0, 120)}`);
  }
}

// Batch: enviar invitación a todos los pendientes (stage: nuevo)
async function sendInviteToAll(from) {
  try {
    const all = await redis.hgetall('wedding:guests');
    const pendientes = Object.entries(all)
      .map(([phone, raw]) => ({ phone, ...JSON.parse(raw) }))
      .filter(g => g.stage === 'nuevo');
    if (!pendientes.length) {
      await sendWhatsAppMessage(from, '✅ No hay invitados pendientes (todos tienen invitación enviada o ya respondieron).');
      return;
    }
    let ok = 0, fail = 0;
    let mediaId = null;
    for (const g of pendientes) {
      try {
        if (!mediaId) mediaId = await uploadImageToMeta(SAVE_THE_DATE_IMG_URL); // subir foto una vez
        const result = await sendInviteTemplate(g.phone, mediaId);
        if (result?.messages?.[0]?.id) {
          await recordTemplateSent(g.phone, SAVE_THE_DATE_TEMPLATE, result.messages[0].id);
          await updateGuestStage(g.phone, 'invitacion_enviada');
          ok++;
        } else {
          fail++;
        }
      } catch (e) {
        console.error(`❌ batch invite ${g.phone}:`, e.message);
        fail++;
      }
      await new Promise(r => setTimeout(r, 300)); // rate limit
    }
    await sendWhatsAppMessage(from, `📨 *Batch completado:* enviadas ${ok} / fallidas ${fail} / total pendientes ${pendientes.length}`);
    await notifySlack(`📨 *Batch invitaciones:* ${ok} enviadas, ${fail} fallidas`);
  } catch (e) {
    console.error('❌ sendInviteToAll error:', e.message);
    await sendWhatsAppMessage(from, '⚠️ Error en el batch de invitaciones.');
  }
}

// ── DeepSeek-based RSVP Classification ───────────────────────
// Uses DeepSeek Flash (deepseek-chat) — regla permanente: sin Anthropic
// Falls back to negation-aware heuristic if DEEPSEEK_API_KEY not set

async function classifyRSVPIntent(text) {
  if (!DEEPSEEK_API_KEY) {
    return heuristicRSVP(text);
  }

  try {
    const res = await axios.post('https://api.deepseek.com/v1/chat/completions', {
      model: DEEPSEEK_MODEL,
      max_tokens: 5,
      messages: [
        { role: 'system', content: 'Clasificá el mensaje como "confirm" (SÍ asiste), "decline" (NO asiste), o "unknown" (no está claro).\n\n⚠️ "no voy" = decline. "no voy a poder" = decline. "no puedo confirmar todavía" = unknown. "sí, voy" = confirm. "dale, ahí estaré" = confirm.\n\nRespondé SOLO una palabra: confirm, decline, o unknown.' },
        { role: 'user', content: text }
      ]
    }, {
      headers: {
        'Authorization': 'Bearer ' + DEEPSEEK_API_KEY,
        'Content-Type': 'application/json',
      },
      timeout: 10000
    });

    const classification = (res.data?.choices?.[0]?.message?.content || '').trim().toLowerCase();
    console.log(`🤖 DeepSeek RSVP: "${text.slice(0, 80)}" → ${classification}`);
    return classification || 'unknown';
  } catch (err) {
    console.error('❌ DeepSeek RSVP error:', err.message);
    return heuristicRSVP(text);
  }
}

function heuristicRSVP(text) {
  const lower = text.toLowerCase();

  // Negation-aware: check "no" patterns FIRST
  if (/\bno\s+(?:podr[eé]|puedo|voy\b|pasa|queda|alcanzo|creo\s+que\s+podr[eé])/i.test(lower)) return 'decline';
  if (/\bno\s+(?:voy\s+a\s+poder|creo\s+poder|estar[eé])/i.test(lower)) return 'decline';

  // Strong confirm patterns (checked AFTER negation)
  if (/\ball[ií]\s+estar[eé]/i.test(lower)) return 'confirm';
  if (/\bcontad(?:lo|la|nos|me)\s+conmigo/i.test(lower)) return 'confirm';
  if (/\b(?:yo\s+)?me\s+(?:apunto|sumo)\b/i.test(lower)) return 'confirm';
  if (/\bvoy\s+seguro|seguro\s+que\s+(?:voy|ir[eé])|confirm(?:ado|o\b|amos\b)/i.test(lower)) return 'confirm';

  return 'unknown';
}

async function handleTextRSVP(from, text) {
  const intent = await classifyRSVPIntent(text);

  if (intent === 'confirm') {
    const confirmMsg = `¡Gracias por confirmar, nos alegra mucho! 🎉\n\n📅 Agregá el evento a tu calendario:\n${TENANT.calendarUrl}\n\n📍 ${TENANT.lugar}\n🕕 ${TENANT.hora} hrs (${TENANT.horaNota})\n👔 ${TENANT.dressCode}\n\nPronto te llegará la invitación formal. ¡Nos vemos! ✨`;
    await sendWhatsAppMessage(from, confirmMsg);
    await saveRSVP(from, '✅ Confirmado (texto)', text);
    await updateGuestStage(from, 'confirmado'); // F1
    await notifySlack(`🎉 *RSVP CONFIRMADO (LLM)* \`${from}\`: "${text.slice(0, 100)}"`);
  } else if (intent === 'decline') {
    const declineMsg = `Gracias por avisarnos, lo entendemos completamente 🫶\n\nTe tendremos presente ese día. ¡Un abrazo!`;
    await sendWhatsAppMessage(from, declineMsg);
    await saveRSVP(from, '❌ No asistirá (texto)', text);
    await updateGuestStage(from, 'no_asistira'); // F1
    await notifySlack(`💔 *NO ASISTIRÁ (LLM)* \`${from}\`: "${text.slice(0, 100)}"`);
  } else {
    // Unknown intent — use Claude to generate a natural reply
    console.log(`🤷 RSVP unknown: ${from} — "${text.slice(0, 80)}"`);
    await generateAndSendDeepSeekReply(from, text);
  }
}

// ── DeepSeek-Generated Auto-Reply ────────────────────────────
// Generates a natural conversational reply for non-RSVP messages

async function generateAndSendDeepSeekReply(phone, userText) {
  if (!DEEPSEEK_API_KEY) return;

  try {
    const res = await axios.post('https://api.deepseek.com/v1/chat/completions', {
      model: DEEPSEEK_MODEL,
      max_tokens: 300,
      messages: [
        { role: 'system', content: `Sos el asistente de WhatsApp para la boda de ${TENANT.novios.nombre1} y ${TENANT.novios.nombre2}.

Contexto de la boda:
- Fecha: ${new Date(TENANT.fecha).toLocaleDateString('es-CL', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}
- Hora: ${TENANT.hora} hrs
- Lugar: ${TENANT.lugar}
- Dress code: ${TENANT.dressCode}

Reglas:
1. SIEMPRE responde en el mismo idioma del invitado (español si escribe español, inglés si escribe inglés)
2. Tono: directo, sin rodeos, informal pero respetuoso. NADA de "espero que estés bien", "saludos cordiales", "quedo atento". Sin chilenismos (nada de "cachai", "puta", "weón")
3. Si preguntan fecha/hora/lugar/ubicación → responde con los datos concretos. Incluí el link del calendario: ${TENANT.calendarUrl}
4. Si NO es pregunta sobre la boda → redirigí amablemente: "Para confirmar tu asistencia usá los botones de arriba, o decime 'voy' o 'no voy a poder'"
5. Máximo 3 oraciones. Breve y útil.
6. Respuestas de UNA SOLA LÍNEA cuando sea posible.` },
        { role: 'user', content: userText }
      ]
    }, {
      headers: {
        'Authorization': 'Bearer ' + DEEPSEEK_API_KEY,
        'Content-Type': 'application/json',
      },
      timeout: 15000
    });

    const reply = (res.data?.choices?.[0]?.message?.content || '').trim();
    if (reply) {
      await sendWhatsAppMessage(phone, reply);
      await notifySlack(`💬 *DeepSeek reply* a \`${phone}\`: "${userText.slice(0, 80)}" → "${reply.slice(0, 100)}"`);
      console.log(`💬 DeepSeek reply sent to ${phone}`);
    }
  } catch (err) {
    console.error('❌ DeepSeek reply error:', err.message);
    // Silent fail — don't spam guest with errors
  }
}

// ── Button Reply Handler ─────────────────────────────────────
async function handleButtonReply(from, buttonId, buttonText) {
  const isConfirm = buttonId === 'Confirmar asistencia' || buttonId === 'confirmar_asistencia';
  const isDecline = buttonId === 'No podre asistir' || buttonId === 'no_asistire';

  if (isConfirm) {
    const confirmMsg = `¡Gracias por confirmar, nos alegra mucho! 🎉\n\n📅 Agregá el evento a tu calendario:\n${TENANT.calendarUrl}\n\n📍 ${TENANT.lugar}\n🕕 ${TENANT.hora} hrs (${TENANT.horaNota})\n👔 ${TENANT.dressCode}\n\nPronto te llegará la invitación formal. ¡Nos vemos! ✨`;
    await sendWhatsAppMessage(from, confirmMsg);
    await saveRSVP(from, '✅ Confirmado (botón)', buttonText);
    await updateGuestStage(from, 'confirmado'); // F1
    await notifySlack(`🎉 *RSVP CONFIRMADO* \`${from}\``);

  } else if (isDecline) {
    const declineMsg = `Gracias por avisarnos, lo entendemos completamente 🫶\n\nTe tendremos presente ese día. ¡Un abrazo!`;
    await sendWhatsAppMessage(from, declineMsg);
    await saveRSVP(from, '❌ No asistirá (botón)', buttonText);
    await updateGuestStage(from, 'no_asistira'); // F1
    await notifySlack(`💔 *NO ASISTIRÁ* \`${from}\``);
  }
}

// ── Save RSVP to Redis ───────────────────────────────────────
async function saveRSVP(phone, status, rawReply) {
  try {
    // Check for existing RSVP from this phone (upsert)
    const entries = await redis.lrange(RSVP_KEY, 0, -1);
    let foundIdx = -1;
    for (let i = 0; i < entries.length; i++) {
      const e = JSON.parse(entries[i]);
      if (e.telefono === phone) {
        foundIdx = i;
        break;
      }
    }

    const entry = JSON.stringify({
      timestamp: new Date().toISOString(),
      telefono: phone,
      rsvp: status,
      notas: rawReply || '',
      updated: foundIdx >= 0 ? 'actualizado' : 'nuevo',
    });

    if (foundIdx >= 0) {
      // Replace existing entry
      await redis.lset(RSVP_KEY, foundIdx, entry);
      console.log(`📊 RSVP updated: ${phone} → ${status}`);
    } else {
      await redis.rpush(RSVP_KEY, entry);
      console.log(`📊 RSVP saved: ${phone} → ${status} (total: ${await redis.llen(RSVP_KEY)})`);
    }
  } catch (err) {
    console.error('❌ Redis save error:', err.message);
  }
}

// ── Slack utilities ──────────────────────────────────────────
async function notifySlack(text) {
  if (!SLACK_BOT_TOKEN || !SLACK_CHANNEL_ID) return;
  try {
    await axios.post(`${SLACK_API}/chat.postMessage`, {
      channel: SLACK_CHANNEL_ID,
      text,
      mrkdwn: true,
    }, {
      headers: {
        Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
        'Content-Type': 'application/json',
      },
    });
  } catch (err) {
    console.error('Slack notify error:', err.message);
  }
}

async function notifySlackReply(threadTs, text) {
  if (!SLACK_BOT_TOKEN || !SLACK_CHANNEL_ID) return;
  try {
    await axios.post(`${SLACK_API}/chat.postMessage`, {
      channel: SLACK_CHANNEL_ID,
      text,
      thread_ts: threadTs,
      mrkdwn: true,
    }, {
      headers: {
        Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
        'Content-Type': 'application/json',
      },
    });
  } catch (err) {
    console.error('Slack reply error:', err.message);
  }
}

async function getOrCreateSlackThread(phone) {
  // Retorna { thread_ts, headerTs } creando el thread si no existe (estilo Yeppo)
  const existing = await redis.hget(CONVERSATION_KEY, `slack:thread:${phone}`);
  if (existing) {
    try {
      const data = JSON.parse(existing);
      // Thread expirado (>24h)
      if (Date.now() - (data.timestamp || 0) < 24 * 60 * 60 * 1000) {
        return data;
      }
    } catch (e) { /* ignore */ }
  }

  // Crear header del thread con el teléfono visible
  const headerBase = `📱 *+${phone}*`;
  const headerText = `🟡 ${headerBase}\n*Estado:* En curso — bot respondiendo\n\nComandos: \`tomar\` · \`soltar\``;
  const res = await axios.post(`${SLACK_API}/chat.postMessage`, {
    channel: SLACK_CHANNEL_ID,
    text: headerText,
    mrkdwn: true,
    blocks: [{ type: 'section', text: { type: 'mrkdwn', text: headerText } }],
  }, {
    headers: {
      Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
      'Content-Type': 'application/json',
    },
  });

  if (res.data?.ts) {
    const data = { thread_ts: res.data.ts, headerTs: res.data.ts, headerBase, channel: SLACK_CHANNEL_ID, timestamp: Date.now() };
    await redis.hset(CONVERSATION_KEY, `slack:thread:${phone}`, JSON.stringify(data));
    await redis.expire(CONVERSATION_KEY, 86400 * 30);
    return data;
  }
  return null;
}

async function sendToSlack(from, text, timestamp, fromPhone, buttonId, wamid) {
  if (!SLACK_BOT_TOKEN || !SLACK_CHANNEL_ID) return;
  try {
    const msgTime = new Date(parseInt(timestamp) * 1000).toISOString();
    let slackMsg = `💬 *Invitado:* ${text}`;
    if (buttonId) slackMsg += `\n> *Botón:* \`${buttonId}\``;
    slackMsg += `\n> *Hora:* ${msgTime}`;

    // Thread estilo Yeppo: header + reply
    const thread = await getOrCreateSlackThread(from);
    if (thread?.thread_ts) {
      await axios.post(`${SLACK_API}/chat.postMessage`, {
        channel: SLACK_CHANNEL_ID,
        thread_ts: thread.thread_ts,
        text: slackMsg,
        mrkdwn: true,
      }, {
        headers: {
          Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
          'Content-Type': 'application/json',
        },
      });
    } else {
      // Fallback: mensaje suelto (si no hay thread)
      await axios.post(`${SLACK_API}/chat.postMessage`, {
        channel: SLACK_CHANNEL_ID,
        text: `${slackMsg}\n\n> ${from}`,
        mrkdwn: true,
      }, {
        headers: {
          Authorization: `Bearer ${SLACK_BOT_TOKEN}`,
          'Content-Type': 'application/json',
        },
      });
    }

    if (wamid) {
      try {
        await redis.hset(SLACK_TS_KEY, wamid, thread?.thread_ts || '');
        await redis.expire(SLACK_TS_KEY, 86400 * 30);
      } catch (e) { /* ignore */ }
    }
  } catch (err) {
    console.error('Slack send error:', err.message);
  }
}

// ── Auto-Reply ────────────────────────────────────────────────
async function sendAutoReply(to, text) {
  let reply = null;
  const lower = text.toLowerCase();

  if (/hola|buenas|ola|holi|hey|info/i.test(lower) && lower.length < 20) {
    reply = `¡Hola! 💒 Somos ${TENANT.novios.nombre1} y ${TENANT.novios.nombre2}.\n\nNos casamos el *17 de noviembre de 2026* a las *${TENANT.hora}* (${TENANT.horaNota}) en *${TENANT.lugar}*.\n\n🎎 Celebración boda China / Coreana — dress code *${TENANT.dressCode}*.\n\nPronto recibirás la invitación formal. Mientras tanto, puedes visitar nuestro sitio: ${TENANT.siteUrl}`;
  } else if (/fecha|cu[aá]ndo|d[ií]a\b.*(?:boda|casamiento|evento)/i.test(lower)) {
    reply = `📅 Nos casamos el *17 de noviembre de 2026* a las *${TENANT.hora}* (${TENANT.horaNota})\n📍 ${TENANT.lugar}\n👗 ${TENANT.dressCode}`;
  } else if (/lugar|d[oó]nde|ubicaci[oó]n|direcci[oó]n/i.test(lower)) {
    reply = `📍 ${TENANT.lugar}\n\n🗺️ Google Maps: https://maps.google.com/?q=Restaurante+Meihua+Cerrillos`;
  }

  if (reply) {
    await sendWhatsAppMessage(to, reply);
  }
}

// ── Send WhatsApp Template con IMAGE header ──
// v4_img: header IMAGE + 3 variables de body. v5_img: además 2 botones URL dinámicos ({{1}} = teléfono).
async function uploadImageToMeta(imageUrl) {
  const imgRes = await axios.get(imageUrl, { responseType: 'arraybuffer', timeout: 30000 });
  const img = Buffer.from(imgRes.data);
  const boundary = '----WebKitFormBoundary' + Math.random().toString(16).slice(2, 14);
  const head = Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="messaging_product"\r\n\r\nwhatsapp\r\n` +
    `--${boundary}\r\nContent-Disposition: form-data; name="type"\r\n\r\nimage/jpeg\r\n` +
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="foto-pareja.jpg"\r\nContent-Type: image/jpeg\r\n\r\n`
  );
  const tail = Buffer.from(`\r\n--${boundary}--\r\n`);
  const body = Buffer.concat([head, img, tail]);
  const res = await axios.post(`${META_API}/${PHONE_NUMBER_ID}/media`, body, {
    headers: {
      Authorization: `Bearer ${WHATSAPP_TOKEN}`,
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
    },
    timeout: 30000,
  });
  return res.data.id;
}

async function sendInviteTemplate(to, mediaId) {
  // Body: "Nos casamos el {{1}} de {{2}} de {{3}}." → 17 / noviembre / 2026
  const [anio, mesNum, dia] = (TENANT.fecha || '2026-11-17').split('-');
  const meses = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
  const components = [
    { type: 'header', parameters: [{ type: 'image', image: { id: mediaId } }] },
    { type: 'body', parameters: [
      { type: 'text', text: String(parseInt(dia, 10)) },
      { type: 'text', text: meses[parseInt(mesNum, 10) - 1] || 'noviembre' },
      { type: 'text', text: anio },
    ]},
  ];
  // v5_img: 2 botones URL dinámicos. Cada botón usa {{1}} = teléfono del invitado (variable propia por botón).
  if (SAVE_THE_DATE_TEMPLATE.includes('v5')) {
    const phoneParam = String(to).replace(/^\+/, '');
    components.push(
      { type: 'button', sub_type: 'url', index: '0', parameters: [{ type: 'text', text: phoneParam }] },
      { type: 'button', sub_type: 'url', index: '1', parameters: [{ type: 'text', text: phoneParam }] },
    );
  }
  const res = await axios.post(`${META_API}/${PHONE_NUMBER_ID}/messages`, {
    messaging_product: 'whatsapp',
    recipient_type: 'individual',
    to: to,
    type: 'template',
    template: {
      name: SAVE_THE_DATE_TEMPLATE,
      language: { code: 'es' },
      components,
    },
  }, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
    timeout: 30000,
  });
  return res.data;
}

// ── Send WhatsApp Message ─────────────────────────────────────
async function sendWhatsAppMessage(to, text) {
  if (!WHATSAPP_TOKEN || !PHONE_NUMBER_ID) {
    console.warn('⚠️ WhatsApp not configured');
    return null;
  }
  try {
    const res = await axios.post(`${META_API}/${PHONE_NUMBER_ID}/messages`, {
      messaging_product: 'whatsapp',
      recipient_type: 'individual',
      to: to,
      type: 'text',
      text: { body: text },
    }, {
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
        'Content-Type': 'application/json',
      },
    });
    const msgId = res.data?.messages?.[0]?.id;
    console.log(`✅ Sent to ${to}: ${msgId || 'ok'}`);
    return res.data;
  } catch (err) {
    const error = err.response?.data || err.message;
    console.error(`❌ Failed to send to ${to}:`, JSON.stringify(error).slice(0, 200));
    return null;
  }
}

// ── Send WhatsApp Template ────────────────────────────────────
async function sendTemplate(to, templateName, params = []) {
  if (!WHATSAPP_TOKEN || !PHONE_NUMBER_ID) {
    console.warn('⚠️ WhatsApp not configured');
    return null;
  }
  try {
    const bodyParams = params.map((p) => ({
      type: 'text',
      text: String(p),
    }));

    const res = await axios.post(`${META_API}/${PHONE_NUMBER_ID}/messages`, {
      messaging_product: 'whatsapp',
      recipient_type: 'individual',
      to: to,
      type: 'template',
      template: {
        name: templateName,
        language: { code: 'es' },
        components: [
          {
            type: 'body',
            parameters: bodyParams,
          },
        ],
      },
    }, {
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
        'Content-Type': 'application/json',
      },
    });
    console.log(`📨 Template "${templateName}" sent to ${to}`);
    return res.data;
  } catch (err) {
    console.error(`❌ Template "${templateName}" failed for ${to}:`, err.response?.data || err.message);
    return null;
  }
}

// ─────────────── ADMIN ENDPOINTS ──────────────────────────────

app.get('/admin/config', (_req, res) => {
  res.json({
    tenant: TENANT,
    configured: {
      whatsappToken: !!WHATSAPP_TOKEN,
      phoneNumberId: !!PHONE_NUMBER_ID,
      slackBotToken: !!SLACK_BOT_TOKEN,
      slackChannelId: !!SLACK_CHANNEL_ID,
      slackSigningSecret: !!SLACK_SIGNING_SECRET,
      verifyToken: !!VERIFY_TOKEN,
      redis: redis.status === 'ready',
    },
    slackEventUrl: SLACK_SIGNING_SECRET
      ? 'https://claw-wedding-agent-production.up.railway.app/slack/events'
      : '⚠️ Configurar SLACK_SIGNING_SECRET',
  });
});

app.post('/admin/test-message', async (req, res) => {
  const { to, text } = req.body;
  if (!to) return res.status(400).json({ error: 'Phone number required' });
  const result = await sendWhatsAppMessage(to, text || '🧪 Mensaje de prueba — claw-wedding-agent v1.4');
  res.json({ sent: !!result, result });
});

app.post('/admin/test-template', async (req, res) => {
  const { to, template, params } = req.body;
  if (!to || !template) return res.status(400).json({ error: 'Phone and template required' });
  const result = await sendTemplate(to, template, params || []);
  res.json({ sent: !!result, result });
});

// NEW: Simulate an incoming WhatsApp webhook (for testing the full flow)
app.post('/admin/simulate-webhook', async (req, res) => {
  const { from, type, text, buttonId } = req.body;

  if (!from) return res.status(400).json({ error: 'Phone number (from) required' });

  const phone = normalizePhone(from);

  if (type === 'text') {
    const msg = { from: phone, timestamp: Math.floor(Date.now() / 1000), type: 'text', text: { body: text || 'Hola' } };
    await handleIncomingMessage(msg, '5497 (simulado)');
    return res.json({ ok: true, simulated: 'text', from: phone, text });

  } else if (type === 'button') {
    const btnId = buttonId || 'Confirmar asistencia';
    const btnTitle = buttonId === 'no_asistire' || buttonId === 'No podre asistir' ? 'No podre asistir' : 'Confirmar asistencia';
    const msg = {
      from: phone,
      timestamp: Math.floor(Date.now() / 1000),
      type: 'interactive',
      interactive: { type: 'button_reply', button_reply: { id: btnId, title: btnTitle } },
    };
    await handleIncomingMessage(msg, '5497 (simulado)');
    return res.json({ ok: true, simulated: 'button', from: phone, button: btnId });

  } else {
    return res.status(400).json({ error: 'Type must be "text" or "button"' });
  }
});

// NEW: Simulate multiple RSVPs at once (batch test)
app.post('/admin/simulate-batch', async (req, res) => {
  const { phones } = req.body;
  if (!phones || !Array.isArray(phones)) return res.status(400).json({ error: 'Array of phone numbers required' });

  const results = [];
  for (const entry of phones) {
    const phone = normalizePhone(typeof entry === 'string' ? entry : entry.phone);
    const rsvp = entry.rsvp || '✅ Confirmado (botón)';

    if (entry.rsvp === 'no') {
      const msg = {
        from: phone,
        timestamp: Math.floor(Date.now() / 1000),
        type: 'interactive',
        interactive: { type: 'button_reply', button_reply: { id: 'No podre asistir', title: 'No podre asistir' } },
      };
      await handleIncomingMessage(msg, '5497 (batch)');
      results.push({ phone, rsvp: 'declined' });
    } else {
      const msg = {
        from: phone,
        timestamp: Math.floor(Date.now() / 1000),
        type: 'interactive',
        interactive: { type: 'button_reply', button_reply: { id: 'Confirmar asistencia', title: 'Confirmar asistencia' } },
      };
      await handleIncomingMessage(msg, '5497 (batch)');
      results.push({ phone, rsvp: 'confirmed' });
    }
  }

  res.json({ ok: true, processed: results.length, results });
});

// NEW: Send from Slack — trigger WhatsApp message
// Use this with Slack Outgoing Webhook or Slash Command
app.post('/admin/send-from-slack', async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'Text required (format: +569XXXXXXXX mensaje)' });

  const phoneMatch = text.match(PHONE_RE_SINGLE);
  if (!phoneMatch) return res.status(400).json({ error: 'No phone number found. Format: +569XXXXXXXX mensaje' });

  const phone = normalizePhone(phoneMatch[0]);
  const message = text.replace(phoneMatch[0], '').trim();

  if (!message) return res.status(400).json({ error: 'Empty message after phone number' });

  const result = await sendWhatsAppMessage(phone, message);
  res.json({
    sent: !!result,
    to: phone,
    message: message.slice(0, 100),
    result: result ? 'accepted' : 'failed',
  });
});

// List all RSVPs
app.get('/admin/rsvps', async (_req, res) => {
  try {
    const entries = await redis.lrange(RSVP_KEY, 0, -1);
    const rsvps = entries.map(e => JSON.parse(e));
    res.json({ total: rsvps.length, rsvps });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /admin/rsvps/repair — corrige acompañantes históricos (bug ñ en parser, 11-ago-2026)
app.post('/admin/rsvps/repair', async (_req, res) => {
  try {
    const all = await redis.hgetall(CONVERSATION_KEY);
    const entries = await redis.lrange(RSVP_KEY, 0, -1);
    const rsvps = entries.map(e => JSON.parse(e));
    const corregidos = [];
    // recorrer conversaciones con form RSVP y re-parsear con el parser corregido
    for (const [key, value] of Object.entries(all)) {
      if (!key.startsWith('wa:')) continue;
      const conv = JSON.parse(value);
      const text = conv.lastMessage || '';
      if (!text.includes('Confirmo mi asistencia')) continue;
      const d = parseRsvpForm(text);
      const acomp = d.acompanantes || '0';
      const telefono = key.slice(3);
      // buscar el RSVP de ese teléfono y actualizar acompañantes
      for (const r of rsvps) {
        const rTel = (r.telefono || '').replace(/[^\d]/g, '');
        const cTel = telefono.replace(/[^\d]/g, '');
        if (rTel === cTel && r.notas === 'Form micrositio') {
          const antes = r.acompanantes;
          // normalizar SIEMPRE desde el texto original (re-parse con parser corregido)
          const nuevo = parseRsvpForm(text).acompanantes || '0';
          if (String(antes) !== String(nuevo)) {
            r.acompanantes = nuevo;
            corregidos.push({ telefono: cTel, nombre: r.nombre, antes, ahora: nuevo });
          }
        }
      }
    }
    // reescribir la lista completa en Redis
    await redis.del(RSVP_KEY);
    for (const r of rsvps) {
      await redis.rpush(RSVP_KEY, JSON.stringify(r));
    }
    res.json({ ok: true, corregidos: corregidos.length, detalle: corregidos });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Stats: confirmed vs declined
app.get('/admin/stats', async (_req, res) => {
  try {
    const s = await getConfirmedStats(); // con absorción de parejas 👫
    res.json({ total: s.totalRegistrados, confirmed: s.confirmed, declined: s.declined, maybe: s.maybe, totalAsistentes: s.totalAsistentes, pending: s.totalRegistrados - s.confirmed - s.declined - s.maybe });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// G1: DELETE /admin/guests/{phone} — eliminar invitado (Opción B: también su RSVP)
app.delete('/admin/guests/:phone', async (req, res) => {
  try {
    const phone = normalizePhone(req.params.phone);
    const guest = await getGuest(phone);
    if (!guest) return res.status(404).json({ error: `No existe el invitado ${phone}` });
    await deleteGuest(phone);
    res.json({ ok: true, deleted: { phone, name: guest.name, stage: guest.stage } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// NEW: Conversation info (for Slack reply context)
app.get('/admin/conversations', async (_req, res) => {
  try {
    const all = await redis.hgetall(CONVERSATION_KEY);
    const waConversations = {};
    for (const [key, value] of Object.entries(all)) {
      if (key.startsWith('wa:')) {
        waConversations[key.slice(3)] = JSON.parse(value);
      }
    }
    res.json({ total: Object.keys(waConversations).length, conversations: waConversations });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Fase 7: LEAD endpoints ───────────────────────────────────
// POST /api/lead — form del sitio producto (noscasamos.vip/contacto)
app.post('/api/lead', async (req, res) => {
  const b = req.body || {};
  const lead = {
    phone: b.phone || null,
    email: b.email || null,
    nombres: b.nombres || b.name || null,
    fecha_boda: b.fecha_boda || b.fecha || null,
    ciudad: b.ciudad || null,
    n_invitados: b.n_invitados || b.invitados || null,
    plan_interes: b.plan_interes || b.plan || null,
    mensaje: b.mensaje || b.msg || null,
    origen: b.origen || 'form_sitio',
  };
  if (!lead.phone && !lead.email && !lead.nombres) {
    return res.status(400).json({ error: 'Se requiere al menos phone, email o nombres' });
  }
  const saved = await saveLead(lead);
  if (!saved) return res.status(500).json({ error: 'No se pudo guardar (¿Postgres configurado?)' });
  await notifySlack(`💼 *LEAD FORM SITIO*: ${lead.nombres || '?'}${lead.phone ? ` · ${lead.phone}` : ''}${lead.email ? ` · ${lead.email}` : ''}${lead.fecha_boda ? ` · ${lead.fecha_boda}` : ''}${lead.plan_interes ? ` · Plan ${lead.plan_interes}` : ''}`);
  res.json({ ok: true, id: saved.id });
});

// GET /admin/leads — listar leads (seguimiento marketing)
app.get('/admin/leads', async (_req, res) => {
  const leads = await listLeads();
  res.json({ total: leads.length, leads });
});

// GET /admin/guests — lista de invitados agregados por novios (hash, con stage F1)
app.get('/admin/guests', async (_req, res) => {
  try {
    const all = await redis.hgetall('wedding:guests');
    const guests = Object.entries(all).map(([phone, raw]) => ({ phone, ...JSON.parse(raw) }));
    res.json({ total: guests.length, guests });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /admin/guest-states — resumen de invitados por stage (F1)
app.get('/admin/guest-states', async (_req, res) => {
  try {
    const all = await redis.hgetall('wedding:guests');
    const guests = Object.entries(all).map(([phone, raw]) => ({ phone, ...JSON.parse(raw) }));
    const byStage = {};
    for (const g of guests) {
      const s = g.stage || 'sin_stage';
      byStage[s] = (byStage[s] || 0) + 1;
    }
    res.json({ total: guests.length, byStage, guests: guests.map(g => ({ phone: g.phone, name: g.name, stage: g.stage || 'sin_stage', templatesSent: (g.templatesSent || []).length })) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── CUPO / PREFILL (Ciclo invitados — 15-ago-2026) ──────────────
// CORS: el micrositio (Bluehost) consulta el cupo del invitado por teléfono
app.use('/api/rsvp', (_req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (_req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// GET /api/rsvp/guest?phone=X — devuelve solo {name, phone, acompanantes, hasCupo} para prefill
app.get('/api/rsvp/guest', async (req, res) => {
  try {
    const phone = normalizePhone(String(req.query.phone || ''));
    if (!phone || phone.length < 8) return res.status(400).json({ error: 'phone requerido' });
    const guest = await getGuest(phone);
    if (!guest) return res.json({ found: false, name: null, phone, acompanantes: null, hasCupo: false });
    res.json({
      found: true,
      name: cleanName(guest.name),
      phone: guest.phone || phone,
      acompanantes: typeof guest.acompanantes === 'number' ? guest.acompanantes : null,
      hasCupo: typeof guest.acompanantes === 'number',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /admin/set-cupo — backfill masivo de cupo (body: { cupos: [{phone, acompanantes}, ...] })
// También acepta un invitado individual: { phone, acompanantes }
app.post('/admin/set-cupo', async (req, res) => {
  try {
    const b = req.body || {};
    const items = Array.isArray(b.cupos) ? b.cupos : (b.phone ? [{ phone: b.phone, acompanantes: b.acompanantes }] : []);
    if (!items.length) return res.status(400).json({ error: 'Se requiere { cupos: [{phone, acompanantes}] } o { phone, acompanantes }' });

    const results = [];
    for (const item of items) {
      const phone = normalizePhone(String(item.phone || ''));
      const acompanantes = Math.max(0, Math.min(5, parseInt(item.acompanantes, 10) || 0));
      const guest = await getGuest(phone);
      if (!guest) { results.push({ phone, ok: false, reason: 'no_existe' }); continue; }
      guest.acompanantes = acompanantes;
      guest.cupoSetAt = new Date().toISOString();
      await redis.hset('wedding:guests', phone, JSON.stringify(guest));
      results.push({ phone, name: guest.name, acompanantes, ok: true });
    }
    const ok = results.filter(r => r.ok).length;
    await notifySlack(`🎯 *Cupo backfill*: ${ok}/${results.length} invitados actualizados`);
    res.json({ ok: true, total: results.length, updated: ok, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /admin/clean-names — limpia el prefijo "invitado" de los nombres legacy (one-shot)
app.post('/admin/clean-names', async (req, res) => {
  try {
    const all = await redis.hgetall('wedding:guests');
    const results = [];
    for (const [phone, raw] of Object.entries(all || {})) {
      let guest;
      try { guest = JSON.parse(raw); } catch (e) { continue; }
      if (!guest.name) continue;
      const cleaned = cleanName(guest.name);
      if (cleaned !== guest.name) {
        guest.name = cleaned;
        await redis.hset('wedding:guests', phone, JSON.stringify(guest));
        results.push({ phone, from: guest.name, to: cleaned });
      }
    }
    res.json({ ok: true, updated: results.length, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Código Novios (codigonovios.cl) — Fase C1 ───────────────
// CORS: Bluehost (sitio estático) llama a esta API desde el browser
// F1: agregado PUT — sin él, "Guardar datos"/"Cambiar contraseña" del panel
// fallan en navegador (preflight OPTIONS bloqueado). Fix retroactivo H14.
app.use('/api/codigonovios', (_req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (_req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Minisitio autoadministrable (F1) — CORS propio ──────────
// Editor (Bluehost, mismo origin) + sitio público; el PUT requiere preflight.
app.use('/api/minisitio', (_req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (_req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// GET /api/codigonovios/lista/:slug — lista pública (invitados)
app.get('/api/codigonovios/lista/:slug', async (req, res) => {
  try {
    const slug = (req.params.slug || '').toUpperCase().trim();
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT * FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = r.rows[0];
    if (n.estado === 'pausada') return res.status(403).json({ error: 'Lista pausada' });
    const deseos = await pg.query(
      "SELECT id, nombre, descripcion, foto_url, precio_sugerido, monto_total, monto_recaudado FROM cn_deseos WHERE novio_id = $1 AND estado = 'activo' ORDER BY orden, id",
      [n.id]
    );
    const libres = await pg.query(
      "SELECT COALESCE(SUM(monto_neto),0) AS total FROM cn_regalos WHERE novio_id = $1 AND estado = 'pagado' AND deseo_id IS NULL",
      [n.id]
    );
    const totalRecaudado = deseos.rows.reduce((s, d) => s + (d.monto_recaudado || 0), 0) + parseInt(libres.rows[0].total, 10);
    res.json({
      slug: n.slug,
      novios: `${n.nombre_novio || ''} & ${n.nombre_novia || ''}`,
      fecha_boda: n.fecha_boda,
      activa_hasta: n.activa_hasta,
      total_recaudado: totalRecaudado,
      aportes_libres: parseInt(libres.rows[0].total, 10),
      deseos: deseos.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/codigonovios/regalar — crea preferencia Checkout Pro (paga neto + 10%)
app.post('/api/codigonovios/regalar', async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const montoNeto = parseInt(b.monto_neto, 10);
    const deseoId = b.deseo_id ? parseInt(b.deseo_id, 10) : null;
    if (!slug || !montoNeto || montoNeto < 1000) {
      return res.status(400).json({ error: 'slug y monto_neto (>= 1000) requeridos' });
    }
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT * FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = r.rows[0];
    if (n.estado !== 'activa') return res.status(403).json({ error: 'Lista no activa' });
    if (deseoId) {
      const d = await pg.query('SELECT id FROM cn_deseos WHERE id = $1 AND novio_id = $2', [deseoId, n.id]);
      if (d.rows.length === 0) return res.status(404).json({ error: 'Deseo no encontrado' });
    }
    const comision = Math.round(montoNeto * 0.10);
    const montoTotal = montoNeto + comision;
    const ins = await pg.query(
      `INSERT INTO cn_regalos (deseo_id, novio_id, nombre_invitado, mensaje, monto_neto, comision, monto_total, email_invitado, estado)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pendiente') RETURNING id`,
      [deseoId, n.id, b.nombre_invitado || null, b.mensaje || null, montoNeto, comision, montoTotal, b.email_invitado || null]
    );
    const regaloId = ins.rows[0].id;
    if (!MP_ACCESS_TOKEN) {
      return res.status(503).json({ error: 'Mercado Pago no configurado (MP_ACCESS_TOKEN ausente)', regalo_id: regaloId, monto_total: montoTotal });
    }
    // Crear preferencia Checkout Pro
    const title = `Regalo boda ${n.nombre_novio || ''} & ${n.nombre_novia || ''}`;
    const pref = await axios.post('https://api.mercadopago.com/checkout/preferences', {
      items: [{ title, quantity: 1, unit_price: montoTotal }],
      external_reference: `regalo-${regaloId}`,
      notification_url: `${process.env.PUBLIC_URL || 'https://claw-wedding-agent-production.up.railway.app'}/api/codigonovios/webhook/mp`,
      back_urls: {
        success: `${CN_SITE_URL}/n/${slug}?ok=1`,
        pending: `${CN_SITE_URL}/n/${slug}?pendiente=1`,
        failure: `${CN_SITE_URL}/n/${slug}?fallo=1`,
      },
      auto_return: 'approved',
      payment_methods: { installments: 1 },
    }, { headers: { Authorization: `Bearer ${MP_ACCESS_TOKEN}` } });
    await pg.query('UPDATE cn_regalos SET mp_preference_id = $1 WHERE id = $2', [pref.data.id, regaloId]);
    res.json({ ok: true, regalo_id: regaloId, monto_neto: montoNeto, comision, monto_total: montoTotal, init_point: pref.data.init_point, preference_id: pref.data.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/codigonovios/webhook/mp — confirma pago MP (idempotente por mp_payment_id)
app.post('/api/codigonovios/webhook/mp', async (req, res) => {
  try {
    const body = req.body || {};
    const paymentId = body.data && body.data.id;
    if (!paymentId) return res.status(200).send('ok');
    if (!MP_ACCESS_TOKEN || !pg) return res.status(200).send('ok');
    const pay = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
      headers: { Authorization: `Bearer ${MP_ACCESS_TOKEN}` },
    });
    const p = pay.data;
    if (p.status !== 'approved') return res.status(200).send('ok');
    const m = (p.external_reference || '').match(/^regalo-(\d+)$/);
    if (!m) return res.status(200).send('ok');
    const regaloId = parseInt(m[1], 10);
    // Idempotencia: solo marca pagado si aún no tiene este payment id
    const up = await pg.query(
      `UPDATE cn_regalos SET estado = 'pagado', pagado_at = NOW(), mp_payment_id = $1
       WHERE id = $2 AND mp_payment_id IS NULL AND estado = 'pendiente'
       RETURNING id, novio_id, deseo_id, monto_neto`,
      [String(paymentId), regaloId]
    );
    if (up.rows.length > 0) {
      const g = up.rows[0];
      // Backfill email del invitado desde Checkout Pro si no se capturó en el form
      const payerEmail = (p.payer && p.payer.email) || null;
      if (payerEmail) {
        await pg.query('UPDATE cn_regalos SET email_invitado = COALESCE(email_invitado, $1) WHERE id = $2', [payerEmail, regaloId]);
      }
      if (g.deseo_id) {
        await pg.query('UPDATE cn_deseos SET monto_recaudado = monto_recaudado + $1 WHERE id = $2', [g.monto_neto, g.deseo_id]);
      }
      await notifySlack(`🎁 *REGALO PAGADO* (Código Novios): $${g.monto_neto.toLocaleString('es-CL')} · regalo #${g.id}`);
      sendGuestConfirmation(regaloId).catch((e) => console.error('confirm mp bg:', e.message));
      // TODO C2: avisar al novio por WhatsApp
    }
    res.status(200).send('ok');
  } catch (err) {
    console.error('MP webhook error:', err.message);
    res.status(200).send('ok');
  }
});

// GET /admin/cn/detalle/:slug — panel novios (regalos recibidos + total) — requiere token
app.get('/admin/cn/detalle/:slug', requireAdminToken, async (req, res) => {
  try {
    const slug = (req.params.slug || '').toUpperCase().trim();
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT id, slug, nombre_novio, nombre_novia, fecha_boda, telefono_novio, email, estado, activa_hasta, created_at FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = r.rows[0];
    const regalos = await pg.query('SELECT id, deseo_id, nombre_invitado, mensaje, monto_neto, comision, monto_total, estado, pagado_at, created_at FROM cn_regalos WHERE novio_id = $1 ORDER BY id DESC', [n.id]);
    const deseos = await pg.query('SELECT id, nombre, monto_total, monto_recaudado, estado FROM cn_deseos WHERE novio_id = $1 ORDER BY orden, id', [n.id]);
    const totalPagado = regalos.rows.filter(g => g.estado === 'pagado').reduce((s, g) => s + g.monto_neto, 0);
    res.json({ novio: n, deseos: deseos.rows, regalos: regalos.rows, total_pagado: totalPagado });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /admin/cn/setup — crear/actualizar lista piloto + deseos (idempotente, C1) — requiere secreto admin
app.post('/admin/cn/setup', async (req, res) => {
  try {
    const secret = (req.headers['x-admin-secret'] || '').toString();
    if (secret !== CN_ADMIN_SECRET) return res.status(401).json({ error: 'No autorizado' });
    const b = req.body || {};
    const slug = (b.slug || 'ALEJKUIL').toUpperCase().trim();
    const deseos = Array.isArray(b.deseos) ? b.deseos : [];
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    // Upsert novios
    const up = await pg.query(`
      INSERT INTO cn_novios (slug, nombre_novio, nombre_novia, fecha_boda, telefono_novio, email, password_hash, estado, activa_hasta)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'activa',$8)
      ON CONFLICT (slug) DO UPDATE SET nombre_novio = EXCLUDED.nombre_novio, nombre_novia = EXCLUDED.nombre_novia,
        fecha_boda = EXCLUDED.fecha_boda, telefono_novio = EXCLUDED.telefono_novio, email = EXCLUDED.email,
        password_hash = COALESCE(EXCLUDED.password_hash, cn_novios.password_hash),
        estado = 'activa', activa_hasta = EXCLUDED.activa_hasta, updated_at = NOW()
      RETURNING id, slug
    `, [
      slug,
      b.nombre_novio || null,
      b.nombre_novia || null,
      b.fecha_boda || null,
      b.telefono_novio || null,
      b.email || null,
      b.password ? hashPassword(b.password) : null,
      b.activa_hasta || null
    ]);
    const novioId = up.rows[0].id;
    // Insertar deseos faltantes (solo si trae lista y no existen ya)
    let insertados = 0;
    if (deseos.length > 0) {
      const existing = await pg.query('SELECT nombre FROM cn_deseos WHERE novio_id = $1', [novioId]);
      const existentes = new Set(existing.rows.map(d => d.nombre));
      for (let i = 0; i < deseos.length; i++) {
        const d = deseos[i];
        if (existentes.has(d.nombre)) continue;
        await pg.query(
          'INSERT INTO cn_deseos (novio_id, nombre, descripcion, foto_url, precio_sugerido, monto_total, orden) VALUES ($1,$2,$3,$4,$5,$6,$7)',
          [novioId, d.nombre, d.descripcion || null, d.foto_url || null, d.precio_sugerido || null, d.monto_total || null, i + 1]
        );
        insertados++;
      }
    }
    res.json({ ok: true, novio_id: novioId, slug, deseos_insertados: insertados });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Email de confirmación al invitado (novios@aconcaguacapital.cl) ──
let mailer = null;
function getMailer() {
  if (mailer) return mailer;
  if (!CN_SMTP_USER || !CN_SMTP_PASS) return null;
  mailer = nodemailer.createTransport({
    host: CN_SMTP_HOST,
    port: CN_SMTP_PORT,
    secure: CN_SMTP_PORT === 465 || CN_SMTP_PORT === 443,
    auth: { user: CN_SMTP_USER, pass: CN_SMTP_PASS },
    connectionTimeout: 20000,
  });
  return mailer;
}

async function sendGuestConfirmation(regaloId) {
  try {
    if (!pg) return;
    const r = await pg.query(
      `SELECT r.nombre_invitado, r.email_invitado, r.monto_neto, r.comision, r.monto_total,
              r.notificado_invitado, n.nombre_novio, n.nombre_novia, n.slug
       FROM cn_regalos r JOIN cn_novios n ON n.id = r.novio_id WHERE r.id = $1`,
      [regaloId]
    );
    const g = r.rows[0];
    if (!g || !g.email_invitado || g.notificado_invitado) return;
    const m = getMailer();
    if (!m) { console.warn('📧 Mailer no configurado (CN_SMTP_USER/PASS ausentes)'); return; }
    const novios = [g.nombre_novio, g.nombre_novia].filter(Boolean).join(' & ') || 'los novios';
    const nombre = (g.nombre_invitado || '').trim();
    const fmt = (n) => '$' + Number(n || 0).toLocaleString('es-CL');
    const subject = `¡Gracias por tu regalo${nombre ? ', ' + nombre : ''}! 💍`;
    const text = [
      `¡Gracias${nombre ? ' ' + nombre : ''}! Tu regalo para ${novios} quedó registrado y pagado.`,
      '',
      `• Tu regalo: ${fmt(g.monto_neto)}`,
      `• Comisión de la plataforma (10%): ${fmt(g.comision)}`,
      `• Total pagado: ${fmt(g.monto_total)}`,
      '',
      `Los novios reciben el 100% de tu regalo (${fmt(g.monto_neto)}).`,
      '',
      `Puedes ver la lista aquí: ${CN_SITE_URL}/n/${g.slug}`,
      '',
      '— Código Novios',
    ].join('\n');
    const html = `
      <div style="font-family:Georgia,'Times New Roman',serif;max-width:560px;margin:0 auto;color:#2b2b2b">
        <h2 style="color:#a0674b;margin-bottom:4px">¡Gracias${nombre ? ', ' + nombre : ''}! 💍</h2>
        <p>Tu regalo para <strong>${novios}</strong> quedó registrado y pagado.</p>
        <table style="width:100%;border-collapse:collapse;margin:18px 0">
          <tr><td style="padding:8px 0;border-bottom:1px solid #eee">Tu regalo</td><td style="text-align:right;font-weight:bold">${fmt(g.monto_neto)}</td></tr>
          <tr><td style="padding:8px 0;border-bottom:1px solid #eee;color:#888">Comisión de la plataforma (10%)</td><td style="text-align:right;color:#888">${fmt(g.comision)}</td></tr>
          <tr><td style="padding:8px 0">Total pagado</td><td style="text-align:right;font-weight:bold">${fmt(g.monto_total)}</td></tr>
        </table>
        <p style="color:#555">Los novios reciben el <strong>100% de tu regalo</strong> (${fmt(g.monto_neto)}).</p>
        <p><a href="${CN_SITE_URL}/n/${g.slug}" style="color:#a0674b">Ver la lista de regalos</a></p>
        <p style="color:#999;font-size:12px;margin-top:24px">— Código Novios</p>
      </div>`;
    await m.sendMail({ from: CN_SMTP_FROM, to: g.email_invitado, replyTo: CN_SMTP_REPLY_TO, subject, text, html });
    await pg.query('UPDATE cn_regalos SET notificado_invitado = TRUE WHERE id = $1', [regaloId]);
    console.log(`📧 Confirmación enviada a ${g.email_invitado} (regalo #${regaloId})`);
  } catch (e) {
    console.error('📧 sendGuestConfirmation error:', e.message);
  }
}

// POST /api/codigonovios/admin/reenviar-email/:id — reenvío manual de confirmación (admin)
app.post('/api/codigonovios/admin/reenviar-email/:id', async (req, res) => {
  if (req.headers['x-cn-admin-secret'] !== CN_ADMIN_SECRET) {
    return res.status(401).json({ error: 'no autorizado' });
  }
  const id = parseInt(req.params.id, 10);
  if (!id || !pg) return res.status(400).json({ error: 'id inválido o Postgres no configurado' });
  const r = await pg.query('UPDATE cn_regalos SET notificado_invitado = FALSE WHERE id = $1 RETURNING id', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'regalo no encontrado' });
  await sendGuestConfirmation(id);
  res.json({ ok: true, regalo_id: id });
});

// ── Código Novios — Linkify (transferencia bancaria) ───────────────
// Flujo validado en producción (Yeppo): URL determinística SIN pre-crear cobro.
// Al abrir la URL, Linkify llama GET cobro-info (amount/description) y, al validar
// la transferencia, POST notification con HMAC. Evita el salto a "Validando" por
// cobro pre-existente y el problema del RUT en "Cuenta de origen".

const LINKIFY_PAY_BASE = 'https://app.linkify.cl/pay';

function linkifyPayUrl(invoiceId) {
  return `${LINKIFY_PAY_BASE}/${LINKIFY_MERCHANT}/remote/${invoiceId}`;
}

// HMAC sha256 (hex) sobre contenido crudo; header X-Linkify-Confirmation.
function verifyLinkifyHmac(content, header) {
  if (!LINKIFY_PRIVATE_KEY || !header) return false;
  const expected = crypto.createHmac('sha256', LINKIFY_PRIVATE_KEY).update(content).digest('hex');
  const a = Buffer.from(expected, 'utf8');
  const b = Buffer.from(String(header).toLowerCase(), 'utf8');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function normalizeRut(rut) {
  let r = String(rut || '').replace(/\./g, '').replace(/\s/g, '').toUpperCase();
  const m = /^(\d{1,8})([0-9K])$/.exec(r);
  if (m) r = `${m[1]}-${m[2]}`;
  return r;
}

// POST /api/codigonovios/regalar-linkify — crea fila pendiente y devuelve URL determinística
app.post('/api/codigonovios/regalar-linkify', async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const montoNeto = parseInt(b.monto_neto, 10);
    const deseoId = b.deseo_id ? parseInt(b.deseo_id, 10) : null;
    if (!slug || !montoNeto || montoNeto < 1000) {
      return res.status(400).json({ error: 'slug y monto_neto (>= 1000) requeridos' });
    }
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT * FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = r.rows[0];
    if (n.estado !== 'activa') return res.status(403).json({ error: 'Lista no activa' });
    if (deseoId) {
      const d = await pg.query('SELECT id FROM cn_deseos WHERE id = $1 AND novio_id = $2', [deseoId, n.id]);
      if (d.rows.length === 0) return res.status(404).json({ error: 'Deseo no encontrado' });
    }
    const comision = Math.round(montoNeto * 0.10);
    const montoTotal = montoNeto + comision;
    const ins = await pg.query(
      `INSERT INTO cn_regalos (deseo_id, novio_id, nombre_invitado, mensaje, monto_neto, comision, monto_total, email_invitado, estado)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pendiente') RETURNING id`,
      [deseoId, n.id, b.nombre_invitado || null, b.mensaje || null, montoNeto, comision, montoTotal, b.email_invitado || null]
    );
    const regaloId = ins.rows[0].id;
    const invoiceId = `cn-${regaloId}`;
    await pg.query(`UPDATE cn_regalos SET linkify_invoice_id = $1 WHERE id = $2`, [invoiceId, regaloId]);
    res.json({
      ok: true,
      regalo_id: regaloId,
      monto_neto: montoNeto,
      comision,
      monto_total: montoTotal,
      pay_url: linkifyPayUrl(invoiceId),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/codigonovios/webhook/linkify?encoded_data={"id":"cn-123"} — info del cobro
app.get('/api/codigonovios/webhook/linkify', async (req, res) => {
  try {
    const encoded = req.query.encoded_data;
    if (!verifyLinkifyHmac(encoded, req.headers['x-linkify-confirmation'])) {
      console.error('Linkify cobro-info: HMAC inválido');
      return res.status(401).json({ message: 'Firma inválida' });
    }
    let parsed;
    try { parsed = JSON.parse(encoded); } catch { return res.status(400).json({ message: 'encoded_data inválido' }); }
    const m = /^cn-(\d+)$/.exec(String(parsed.id || ''));
    if (!m) return res.status(400).json({ message: 'id inválido' });
    const regaloId = parseInt(m[1], 10);
    if (!pg) return res.status(500).json({ message: 'Postgres no configurado' });
    const r = await pg.query('SELECT monto_total, novio_id FROM cn_regalos WHERE id = $1', [regaloId]);
    if (r.rows.length === 0) return res.status(400).json({ message: 'No se pudo obtener el cobro', notify_merchant: true });
    const g = r.rows[0];
    const nr = await pg.query('SELECT nombre_novio, nombre_novia FROM cn_novios WHERE id = $1', [g.novio_id]);
    const nov = nr.rows[0] || {};
    const descBase = `Regalo boda ${nov.nombre_novio || ''} & ${nov.nombre_novia || ''}`.trim() || `Regalo #${regaloId}`;
    const description = `${descBase} — Total a transferir: ${Number(g.monto_total).toLocaleString('es-CL')} CLP (incluye comisión 10%)`;
    return res.json({ amount: g.monto_total, description, currency: 'CLP' });
  } catch (err) {
    console.error('Linkify cobro-info error:', err.message);
    return res.status(500).json({ message: 'Error interno' });
  }
});

// POST /api/codigonovios/webhook/linkify — notificación de pago confirmado/anulado
// FAST-ACK: respondemos 200 de inmediato tras validar HMAC + payload y procesamos el
// marcado de pago en background. Evita el "Problema de notificación" de Linkify por
// timeout (misma causa raíz y fix que la integración Yeppo).
app.post('/api/codigonovios/webhook/linkify', async (req, res) => {
  const rawBody = req.rawBody ? req.rawBody.toString('utf8') : JSON.stringify(req.body || {});
  if (!verifyLinkifyHmac(rawBody, req.headers['x-linkify-confirmation'])) {
    console.error('Linkify notification: HMAC inválido');
    return res.status(401).json({ message: 'Firma inválida' });
  }
  const payload = req.body || {};
  const { id, action, completeness, original_amount, transfers } = payload;
  console.log('Linkify notification', { id, action, completeness, original_amount });

  if (action === 'cancellation') {
    return res.json({ status: 'accepted', message: 'Pago anulado' });
  }
  if (action !== 'notification') {
    return res.json({ status: 'accepted', message: 'OK' });
  }
  const m = /^cn-(\d+)$/.exec(String(id || ''));
  if (!m) return res.json({ status: 'accepted', message: 'id no reconocido' });
  const regaloId = parseInt(m[1], 10);

  if (completeness === 'underpaid') {
    processLinkifyNotification(regaloId, { underpaid: true, original_amount, transfers }).catch((e) => console.error('Linkify bg underpaid:', e.message));
    return res.json({ status: 'accepted', message: 'Monto inferior al total del regalo. Transfiere el total exacto.', restart: true });
  }

  // exact / overpaid → fast-ack + redirect a la página de agradecimiento.
  // Linkify redirige al usuario a la URL indicada en `redirect` una vez validado el pago
  // (doc "Pago confirmado (Remoto)" — campo `redirect`). El slug es un SELECT rápido;
  // le ponemos tope de 2s para no romper el fast-ack si la DB se cuelga.
  let redirect = null;
  if (pg) {
    try {
      const slug = await Promise.race([
        (async () => {
          const r = await pg.query('SELECT n.slug FROM cn_regalos r JOIN cn_novios n ON n.id = r.novio_id WHERE r.id = $1', [regaloId]);
          return (r.rows[0] && r.rows[0].slug) || null;
        })(),
        new Promise((_, rej) => setTimeout(() => rej(new Error('slug timeout')), 2000)),
      ]);
      if (slug) redirect = `${CN_SITE_URL}/n/${slug}?ok=1`;
    } catch (e) {
      console.error('Linkify redirect slug:', e.message);
    }
  }
  const resp = { status: 'accepted', message: 'Pago recibido, procesando' };
  if (redirect) resp.redirect = redirect;
  res.json(resp);
  processLinkifyNotification(regaloId, { underpaid: false, original_amount, transfers }).catch((e) => console.error('Linkify bg process:', e.message));
});

// Procesamiento en background del pago Linkify (marcar pagado + Slack).
async function processLinkifyNotification(regaloId, opts) {
  try {
    if (!pg) return;
    const r = await pg.query('SELECT monto_total FROM cn_regalos WHERE id = $1', [regaloId]);
    if (r.rows.length === 0) return;
    const montoTotal = r.rows[0].monto_total;

    // Parse tolerante de montos CLP: acepta "1.100" (miles) y "1100" sin confundirlos con decimales.
    const parseCLP = (v) => {
      if (v == null || v === '') return null;
      if (typeof v === 'number') return Math.round(v);
      const digits = String(v).replace(/[^\d]/g, '');
      return digits ? parseInt(digits, 10) : null;
    };

    if (opts.underpaid || (parseCLP(opts.original_amount) != null && parseCLP(opts.original_amount) < montoTotal)) {
      const recibido = (Array.isArray(opts.transfers) && opts.transfers[0] && opts.transfers[0].amount) || opts.original_amount || '?';
      console.warn(`Linkify underpaid cn-${regaloId}: esperado $${montoTotal}, recibido $${recibido}`);
      try { await notifySlack(`⚠️ Transferencia incompleta (Código Novios) regalo #${regaloId}: se esperaban $${Number(montoTotal).toLocaleString('es-CL')} y llegó $${recibido}. El invitado puede reintentar en Linkify.`); } catch (e) {}
      return;
    }

    const rutInvitado = (Array.isArray(opts.transfers) && opts.transfers[0]) ? normalizeRut(opts.transfers[0].rut) : null;
    // Idempotencia: UPDATE atómico condicional — solo si aún está pendiente.
    const up = await pg.query(
      `UPDATE cn_regalos SET estado = 'pagado', pagado_at = NOW(), rut_invitado = COALESCE($2, rut_invitado)
       WHERE id = $1 AND estado = 'pendiente'
       RETURNING id, novio_id, deseo_id, monto_neto`,
      [regaloId, rutInvitado]
    );
    if (up.rows.length > 0) {
      const g = up.rows[0];
      if (g.deseo_id) {
        await pg.query('UPDATE cn_deseos SET monto_recaudado = monto_recaudado + $1 WHERE id = $2', [g.monto_neto, g.deseo_id]);
      }
      try { await notifySlack(`🏦 *REGALO PAGADO (transferencia)* (Código Novios): $${g.monto_neto.toLocaleString('es-CL')} · regalo #${g.id}`); } catch (e) {}
      sendGuestConfirmation(regaloId).catch((e) => console.error('confirm linkify bg:', e.message));
    }
  } catch (err) {
    console.error('Linkify notification bg error:', err.message);
  }
}

// ── Panel Novios Web (codigonovios.cl/admin.php) — Fase A ───────────

// Hash de contraseña con scrypt (Node nativo, sin dependencias)
function hashPassword(pass) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(pass, salt, 32).toString('hex');
  return `${salt}:${hash}`;
}

function verifyPassword(pass, stored) {
  if (!stored || !stored.includes(':')) return false;
  const [salt, hash] = stored.split(':');
  const test = crypto.scryptSync(pass, salt, 32).toString('hex');
  return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(test, 'hex'));
}

// Token HMAC: slug + expiración, firmado con un secret (CN o minisitio, F1)
function makeAdminToken(slug, secret) {
  const exp = Date.now() + 24 * 3600 * 1000; // 24h
  const payload = `${slug}.${exp}`;
  const sig = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return Buffer.from(`${payload}.${sig}`).toString('base64url');
}

function verifyAdminToken(token, slug, secret) {
  try {
    const decoded = Buffer.from(token, 'base64url').toString();
    const [tokSlug, expStr, sig] = decoded.split('.');
    if (tokSlug !== slug) return false;
    if (Date.now() > parseInt(expStr, 10)) return false;
    const expected = crypto.createHmac('sha256', secret).update(`${tokSlug}.${expStr}`).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(sig, 'hex'), Buffer.from(expected, 'hex'));
  } catch (e) {
    return false;
  }
}

// ── Rate-limit escalonado de login (F1, H5): 5/min → 15/15min → 60/hora ──
// Cada ventana es un contador independiente; al fallar se agota la de 1 min,
// luego la de 15 y finalmente la de 60. Llave = SLUG:IP (nunca lanza).
const cnLoginLimiter60 = rateLimit({ windowMs: 60 * 60 * 1000, limit: 60, standardHeaders: 'draft-7', legacyHeaders: false, keyGenerator: (req) => `${String((req.body && req.body.slug) || 'anon').toUpperCase().slice(0, 20)}:${req.ip}` });
const cnLoginLimiter15 = rateLimit({ windowMs: 15 * 60 * 1000, limit: 15, standardHeaders: 'draft-7', legacyHeaders: false, keyGenerator: (req) => `${String((req.body && req.body.slug) || 'anon').toUpperCase().slice(0, 20)}:${req.ip}` });
const cnLoginLimiter = rateLimit({ windowMs: 60 * 1000, limit: 5, standardHeaders: 'draft-7', legacyHeaders: false, keyGenerator: (req) => `${String((req.body && req.body.slug) || 'anon').toUpperCase().slice(0, 20)}:${req.ip}` });

// Middleware: valida token del panel novios
function requireAdminToken(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const slug = ((req.body && req.body.slug) || req.query.slug || req.params.slug || '').toUpperCase().trim();
  if (!token || !slug || !verifyAdminToken(token, slug, CN_ADMIN_SECRET)) {
    return res.status(401).json({ error: 'No autorizado' });
  }
  next();
}

// Middleware: valida token del editor del minisitio (secret propio, slug = :slug)
function requireMinisitioToken(req, res, next) {
  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  const slug = ((req.params && req.params.slug) || '').toUpperCase().trim();
  if (!token || !slug || !verifyAdminToken(token, slug, MINISITIO_ADMIN_SECRET)) {
    return res.status(401).json({ error: 'No autorizado' });
  }
  next();
}

// POST /api/codigonovios/admin/login — valida contraseña y entrega token 24h
// F1: rate-limit escalonado 5/min → 15/15min → 60/hora (H5, mismo fix que minisitio)
app.post('/api/codigonovios/admin/login', cnLoginLimiter60, cnLoginLimiter15, cnLoginLimiter, async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const pass = b.password || '';
    if (!slug || !pass) return res.status(400).json({ error: 'slug y password requeridos' });
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT password_hash FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    if (!r.rows[0].password_hash) return res.status(403).json({ error: 'Esta lista no tiene contraseña configurada' });
    if (!verifyPassword(pass, r.rows[0].password_hash)) {
      return res.status(401).json({ error: 'Contraseña incorrecta' });
    }
    res.json({ ok: true, token: makeAdminToken(slug, CN_ADMIN_SECRET), slug });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Reseteo de contraseña del panel (código de 6 dígitos al email de la lista) ──
const cnResetLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, limit: 5,
  standardHeaders: 'draft-7', legacyHeaders: false,
  keyGenerator: (req) => `${String((req.body && req.body.slug) || 'anon').toUpperCase().slice(0, 20)}:${req.ip}`,
});

function maskEmail(e) {
  const s = String(e || '');
  const i = s.indexOf('@');
  if (i < 1) return '';
  const u = s.slice(0, i), d = s.slice(i + 1);
  return `${u.slice(0, 1)}${'*'.repeat(Math.max(2, Math.min(6, u.length - 1)))}@${d}`;
}

async function sendResetCodeEmail(to, codigo, nombreLista) {
  const m = getMailer();
  if (!m) throw new Error('mailer no configurado');
  const subject = 'Tu código para recuperar la contraseña — Código Novios';
  const text = [
    `Hola${nombreLista ? ' ' + nombreLista : ''},`,
    '',
    `Tu código para cambiar la contraseña del panel de tu lista es: ${codigo}`,
    '',
    'El código vence en 15 minutos y sirve una sola vez.',
    'Si no pediste este cambio, ignora este correo: tu contraseña sigue igual.',
    '',
    '— Código Novios',
  ].join('\n');
  const html = `
    <div style="font-family:Georgia,'Times New Roman',serif;max-width:520px;margin:0 auto;color:#2b2b2b">
      <h2 style="color:#8B3232;margin-bottom:6px">Recupera tu contraseña</h2>
      <p>Hola${nombreLista ? ' ' + nombreLista : ''}, este es tu código para entrar al panel de tu lista:</p>
      <p style="font-size:32px;letter-spacing:6px;font-weight:bold;color:#8B3232;margin:18px 0">${codigo}</p>
      <p style="color:#555">Vence en <strong>15 minutos</strong> y sirve una sola vez.</p>
      <p style="color:#999;font-size:12px">Si no pediste este cambio, ignora este correo: tu contraseña sigue igual.</p>
      <p style="color:#999;font-size:12px;margin-top:18px">— Código Novios</p>
    </div>`;
  await m.sendMail({ from: CN_SMTP_FROM, to, replyTo: CN_SMTP_REPLY_TO, subject, text, html });
}

// POST /api/codigonovios/admin/reset-solicitar — envía un código de 6 dígitos (15 min)
app.post('/api/codigonovios/admin/reset-solicitar', cnResetLimiter, async (req, res) => {
  try {
    const slug = ((req.body && req.body.slug) || '').toUpperCase().trim();
    if (!slug) return res.status(400).json({ error: 'Ingresa el código de tu lista' });
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT email, nombre_novio, nombre_novia FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = r.rows[0];
    if (!n.email) return res.status(400).json({ error: 'Esta lista no tiene email registrado. Escríbenos a novios@aconcaguacapital.cl y te ayudamos.' });
    const codigo = String(crypto.randomInt(100000, 1000000));
    await pg.query('UPDATE cn_reset_codigos SET usado = TRUE WHERE slug = $1 AND usado = FALSE', [slug]);
    await pg.query(
      "INSERT INTO cn_reset_codigos (slug, codigo_hash, expira) VALUES ($1, $2, NOW() + INTERVAL '15 minutes')",
      [slug, hashPassword(codigo)]
    );
    const nombreLista = [n.nombre_novio, n.nombre_novia].filter(Boolean).join(' & ') || slug;
    try {
      await sendResetCodeEmail(n.email, codigo, nombreLista);
    } catch (e) {
      console.error('📧 reset code email error:', e.message);
      return res.status(502).json({ error: 'No pudimos enviar el correo. Escríbenos a novios@aconcaguacapital.cl.' });
    }
    res.json({ ok: true, email: maskEmail(n.email), expira_min: 15 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/codigonovios/admin/reset-confirmar — valida el código y cambia la contraseña
app.post('/api/codigonovios/admin/reset-confirmar', cnResetLimiter, async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const codigo = String(b.codigo || '').trim();
    const nueva = String(b.password || '').trim();
    if (!slug || !codigo) return res.status(400).json({ error: 'Faltan el código de lista o el código recibido' });
    if (nueva.length < 6) return res.status(400).json({ error: 'La contraseña debe tener al menos 6 caracteres' });
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query(
      'SELECT id, codigo_hash, intentos FROM cn_reset_codigos WHERE slug = $1 AND usado = FALSE AND expira > NOW() ORDER BY id DESC LIMIT 1',
      [slug]
    );
    if (r.rows.length === 0) return res.status(400).json({ error: 'El código expiró o no existe. Pide uno nuevo.' });
    const row = r.rows[0];
    if (row.intentos >= 5) {
      await pg.query('UPDATE cn_reset_codigos SET usado = TRUE WHERE id = $1', [row.id]);
      return res.status(429).json({ error: 'Demasiados intentos con ese código. Pide uno nuevo.' });
    }
    if (!verifyPassword(codigo, row.codigo_hash)) {
      await pg.query('UPDATE cn_reset_codigos SET intentos = intentos + 1 WHERE id = $1', [row.id]);
      return res.status(401).json({ error: 'Código incorrecto' });
    }
    await pg.query('UPDATE cn_reset_codigos SET usado = TRUE WHERE id = $1', [row.id]);
    await pg.query('UPDATE cn_novios SET password_hash = $1, updated_at = NOW() WHERE slug = $2', [hashPassword(nueva), slug]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/codigonovios/admin/panel — novio + deseos + regalos (token)
app.get('/api/codigonovios/admin/panel', requireAdminToken, async (req, res) => {
  try {
    const slug = (req.query.slug || '').toUpperCase().trim();
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const r = await pg.query('SELECT id, slug, nombre_novio, nombre_novia, fecha_boda, telefono_novio, email, estado, activa_hasta, created_at FROM cn_novios WHERE slug = $1', [slug]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = r.rows[0];
    const regalos = await pg.query('SELECT id, deseo_id, nombre_invitado, mensaje, monto_neto, comision, monto_total, mp_payment_id, estado, pagado_at, created_at FROM cn_regalos WHERE novio_id = $1 ORDER BY id DESC', [n.id]);
    const deseos = await pg.query('SELECT id, nombre, descripcion, foto_url, precio_sugerido, monto_total, monto_recaudado, estado, orden FROM cn_deseos WHERE novio_id = $1 ORDER BY orden, id', [n.id]);
    const totalPagado = regalos.rows.filter(g => g.estado === 'pagado').reduce((s, g) => s + g.monto_neto, 0);
    const totalMeta = deseos.rows.reduce((s, d) => s + (d.monto_total || d.precio_sugerido || 0), 0);
    res.json({ novio: n, deseos: deseos.rows, regalos: regalos.rows, total_pagado: totalPagado, total_meta: totalMeta });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/codigonovios/admin/deseos — crear/editar/ocultar/activar (token)
app.post('/api/codigonovios/admin/deseos', requireAdminToken, async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const accion = b.accion || '';
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const nr = await pg.query('SELECT id FROM cn_novios WHERE slug = $1', [slug]);
    if (nr.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const novioId = nr.rows[0].id;

    if (accion === 'crear') {
      const nombre = (b.nombre || '').trim();
      if (!nombre) return res.status(400).json({ error: 'nombre requerido' });
      const ins = await pg.query(
        'INSERT INTO cn_deseos (novio_id, nombre, descripcion, foto_url, precio_sugerido, monto_total, orden) VALUES ($1,$2,$3,$4,$5,$6, COALESCE((SELECT MAX(orden)+1 FROM cn_deseos WHERE novio_id=$1),1)) RETURNING id',
        [novioId, nombre, b.descripcion || null, b.foto_url || null, b.precio_sugerido || null, b.monto_total || null]
      );
      return res.json({ ok: true, deseo_id: ins.rows[0].id });
    }

    const deseoId = parseInt(b.deseo_id, 10);
    if (!deseoId) return res.status(400).json({ error: 'deseo_id requerido' });
    const d = await pg.query('SELECT id FROM cn_deseos WHERE id = $1 AND novio_id = $2', [deseoId, novioId]);
    if (d.rows.length === 0) return res.status(404).json({ error: 'Deseo no encontrado' });

    if (accion === 'editar') {
      await pg.query(
        'UPDATE cn_deseos SET nombre=$1, descripcion=$2, foto_url=$3, precio_sugerido=$4, monto_total=$5 WHERE id=$6',
        [b.nombre || null, b.descripcion !== undefined ? b.descripcion : null, b.foto_url !== undefined ? b.foto_url : null, b.precio_sugerido || null, b.monto_total || null, deseoId]
      );
      return res.json({ ok: true });
    }
    if (accion === 'ocultar' || accion === 'activar') {
      const estado = accion === 'ocultar' ? 'oculto' : 'activo';
      await pg.query('UPDATE cn_deseos SET estado = $1 WHERE id = $2', [estado, deseoId]);
      return res.json({ ok: true, estado });
    }
    if (accion === 'eliminar') {
      await pg.query('DELETE FROM cn_deseos WHERE id = $1', [deseoId]);
      return res.json({ ok: true });
    }
    res.status(400).json({ error: 'accion inválida (crear/editar/ocultar/activar/eliminar)' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/codigonovios/admin/novios — editar datos de la lista (token)
app.put('/api/codigonovios/admin/novios', requireAdminToken, async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    await pg.query(
      `UPDATE cn_novios SET nombre_novio=$1, nombre_novia=$2, fecha_boda=$3, telefono_novio=$4, email=$5, updated_at=NOW() WHERE slug=$6`,
      [b.nombre_novio || null, b.nombre_novia || null, b.fecha_boda || null, b.telefono_novio || null, b.email || null, slug]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/codigonovios/admin/password — cambiar contraseña (token)
app.put('/api/codigonovios/admin/password', requireAdminToken, async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const nueva = (b.password || '').trim();
    if (nueva.length < 6) return res.status(400).json({ error: 'La contraseña debe tener al menos 6 caracteres' });
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    await pg.query('UPDATE cn_novios SET password_hash = $1, updated_at = NOW() WHERE slug = $2', [hashPassword(nueva), slug]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/codigonovios/admin/regalos — eliminar regalos de prueba/errores (token)
app.post('/api/codigonovios/admin/regalos', requireAdminToken, async (req, res) => {
  try {
    const b = req.body || {};
    const slug = (b.slug || '').toUpperCase().trim();
    const accion = b.accion || '';
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const nr = await pg.query('SELECT id FROM cn_novios WHERE slug = $1', [slug]);
    if (nr.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const novioId = nr.rows[0].id;

    if (accion === 'eliminar') {
      const regaloId = parseInt(b.regalo_id, 10);
      if (!regaloId) return res.status(400).json({ error: 'regalo_id requerido' });
      const g = await pg.query('SELECT id, deseo_id, monto_neto, estado FROM cn_regalos WHERE id = $1 AND novio_id = $2', [regaloId, novioId]);
      if (g.rows.length === 0) return res.status(404).json({ error: 'Regalo no encontrado' });
      const regalo = g.rows[0];
      // Si estaba pagado, restar del monto_recaudado del deseo
      if (regalo.estado === 'pagado' && regalo.deseo_id) {
        await pg.query('UPDATE cn_deseos SET monto_recaudado = GREATEST(0, monto_recaudado - $1) WHERE id = $2', [regalo.monto_neto, regalo.deseo_id]);
      }
      await pg.query('DELETE FROM cn_regalos WHERE id = $1', [regaloId]);
      return res.json({ ok: true });
    }

    if (accion === 'reembolsar') {
      const regaloId = parseInt(b.regalo_id, 10);
      if (!regaloId) return res.status(400).json({ error: 'regalo_id requerido' });
      const g = await pg.query('SELECT id, deseo_id, monto_neto, estado FROM cn_regalos WHERE id = $1 AND novio_id = $2', [regaloId, novioId]);
      if (g.rows.length === 0) return res.status(404).json({ error: 'Regalo no encontrado' });
      const regalo = g.rows[0];
      if (regalo.estado === 'pagado' && regalo.deseo_id) {
        await pg.query('UPDATE cn_deseos SET monto_recaudado = GREATEST(0, monto_recaudado - $1) WHERE id = $2', [regalo.monto_neto, regalo.deseo_id]);
      }
      await pg.query("UPDATE cn_regalos SET estado = 'reembolsado' WHERE id = $1", [regaloId]);
      return res.json({ ok: true, estado: 'reembolsado' });
    }

    if (accion === 'eliminar-pendientes') {
      const del = await pg.query('DELETE FROM cn_regalos WHERE novio_id = $1 AND estado = \'pendiente\' RETURNING id', [novioId]);
      return res.json({ ok: true, eliminados: del.rows.length });
    }

    res.status(400).json({ error: 'accion inválida (eliminar / eliminar-pendientes)' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ══════════════════════════════════════════════════════════════
// Minisitio autoadministrable (F1, 27-ago-2026)
// Contrato: tmp/design_minisitio_backend.md §3 · decisiones orquestador: fotos
// Bluehost /uploads/{slug}/, slug unificado bajo codigonovios.cl, draft/publish
// (borrador + botón Publicar) vía columna estado, piloto ALEJKUIL.
//   GET  /api/minisitio/:slug              público (no-cache + ETag + 304)
//   POST /api/minisitio/admin/login        rate-limit escalonado 5/15/60
//   GET  /api/minisitio/admin/:slug        token (compartidos + editable + permisos)
//   PUT  /api/minisitio/admin/:slug        token (whitelist + validación + snapshot + draft/publish)
//   POST /api/minisitio/admin/validate-token  helper para upload.php (Bluehost)
// Regla de oro: NINGÚN endpoint serializa password_hash, banco, tipo_cuenta,
// numero_cuenta, titular, rut_titular (payout) ni email/telefono en GET público.
// ══════════════════════════════════════════════════════════════

// Whitelist estricta de campos editables (spec §3; anti mass-assignment).
// estado/codigo_slug/minisitio_slug/id/novio_id/updated_at NO están → rechazo 400.
const MS_ALLOWED = ['hora','lugar','dress_code','estacionamiento','plus_one','contacto',
                    'hero_img','video_url','aviso','rsvp_plazo',
                    'cronograma','faq','timeline','galeria','regalos'];
const MS_PERMISOS = {
  editable: MS_ALLOWED,
  solo_lectura: ['nombre_novio','nombre_novia','fecha_boda','telefono_novio','email','estado'],
};
const MS_JSONB_COLS = ['cronograma','faq','timeline','galeria','regalos'];
const HHMM_RE = /^([01][0-9]|2[0-3]):[0-5][0-9]$/;
const MS_UUID_JPG_RE = /^[0-9a-f]{32}\.jpg$/;

function msEsUrlHttp(v) {
  try { const u = new URL(String(v)); return u.protocol === 'http:' || u.protocol === 'https:'; } catch (e) { return false; }
}

function msTieneHtml(v) { return /[<>]/.test(String(v)); }

function msTexto(v, max, key, fail) {
  if (v === null || v === undefined) return null;
  if (typeof v !== 'string') fail('debe ser texto');
  const s = v.trim();
  if (s === '') return null;
  if (s.length > max) fail(`máximo ${max} caracteres`);
  if (msTieneHtml(s)) fail('caracteres < > no permitidos');
  return s;
}

function msUrl(v, max, key, fail) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (s === '') return null;
  if (s.length > max) fail(`máximo ${max} caracteres`);
  if (!msEsUrlHttp(s)) fail('solo URLs http(s)');
  return s;
}

function validarMsCronograma(v, key, fail) {
  if (v === null || v === undefined) return [];
  if (!Array.isArray(v)) fail('debe ser array');
  if (v.length > 15) fail('máximo 15 items');
  return v.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) fail('item debe ser objeto');
    for (const k of Object.keys(item)) if (k !== 'hora' && k !== 'actividad') fail(`clave desconocida "${k}"`);
    const hora = item.hora === undefined || item.hora === null ? '' : String(item.hora).trim();
    if (hora !== '' && !HHMM_RE.test(hora)) fail('hora debe ser HH:MM');
    const actividad = String(item.actividad ?? '').trim();
    if (!actividad) fail('actividad requerida');
    if (actividad.length > 300) fail('actividad máx 300 caracteres');
    return { hora, actividad };
  });
}

function validarMsFaq(v, key, fail) {
  if (v === null || v === undefined) return [];
  if (!Array.isArray(v)) fail('debe ser array');
  if (v.length > 10) fail('máximo 10 items');
  return v.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) fail('item debe ser objeto');
    for (const k of Object.keys(item)) if (k !== 'pregunta' && k !== 'respuesta') fail(`clave desconocida "${k}"`);
    const pregunta = String(item.pregunta ?? '').trim();
    const respuesta = String(item.respuesta ?? '').trim();
    if (!pregunta) fail('pregunta requerida');
    if (pregunta.length > 500) fail('pregunta máx 500 caracteres');
    if (!respuesta) fail('respuesta requerida');
    if (respuesta.length > 2000) fail('respuesta máx 2000 caracteres');
    if (msTieneHtml(pregunta) || msTieneHtml(respuesta)) fail('caracteres < > no permitidos');
    return { pregunta, respuesta };
  });
}

function validarMsTimeline(v, key, fail) {
  if (v === null || v === undefined) return [];
  if (!Array.isArray(v)) fail('debe ser array');
  if (v.length > 15) fail('máximo 15 items');
  return v.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) fail('item debe ser objeto');
    for (const k of Object.keys(item)) if (k !== 'fecha' && k !== 'texto' && k !== 'imagen') fail(`clave desconocida "${k}"`);
    const fecha = String(item.fecha ?? '').trim();
    const texto = String(item.texto ?? '').trim();
    if (!fecha || fecha.length > 100) fail('fecha requerida (máx 100)');
    if (!texto || texto.length > 500) fail('texto requerido (máx 500)');
    if (msTieneHtml(fecha) || msTieneHtml(texto)) fail('caracteres < > no permitidos');
    const out = { fecha, texto };
    if (item.imagen !== undefined && item.imagen !== null && String(item.imagen).trim() !== '') {
      const imagen = String(item.imagen).trim();
      if (imagen.length > 2000 || !msEsUrlHttp(imagen)) fail('imagen debe ser URL http(s)');
      out.imagen = imagen;
    }
    return out;
  });
}

function validarMsGaleria(v, key, fail) {
  if (v === null || v === undefined) return [];
  if (!Array.isArray(v)) fail('debe ser array');
  if (v.length > 10) fail('máximo 10 fotos');
  let portadas = 0;
  const out = v.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) fail('item debe ser objeto');
    for (const k of Object.keys(item)) if (k !== 'url' && k !== 'portada') fail(`clave desconocida "${k}"`);
    const url = String(item.url ?? '').trim();
    if (!url || url.length > 2000 || !msEsUrlHttp(url)) fail('url debe ser http(s)');
    const portada = item.portada === true;
    if (portada) portadas++;
    return { url, portada };
  });
  if (portadas > 1) fail('máximo 1 foto marcada como portada');
  return out;
}

function validarMsRegalos(v, key, fail) {
  if (v === null || v === undefined) return {};
  if (typeof v !== 'object' || Array.isArray(v)) fail('debe ser objeto');
  const allowed = ['mesa','codigo','link','tiendas','internacional'];
  for (const k of Object.keys(v)) if (!allowed.includes(k)) fail(`clave desconocida "${k}" en regalos`);
  const out = {};
  out.mesa = v.mesa === true;
  if (v.codigo !== undefined && v.codigo !== null) {
    const c = String(v.codigo).trim().toUpperCase();
    if (c !== '' && !/^[A-Z0-9]{3,12}$/.test(c)) fail('codigo debe ser ^[A-Z0-9]{3,12}$');
    if (c !== '') out.codigo = c;
  }
  if (v.link !== undefined && v.link !== null && String(v.link).trim() !== '') {
    const l = String(v.link).trim();
    if (l.length > 2000 || !msEsUrlHttp(l)) fail('link debe ser URL http(s)');
    out.link = l;
  }
  if (v.tiendas !== undefined && v.tiendas !== null) {
    if (!Array.isArray(v.tiendas)) fail('tiendas debe ser array');
    if (v.tiendas.length > 5) fail('máximo 5 tiendas');
    out.tiendas = v.tiendas.map((t) => {
      if (!t || typeof t !== 'object' || Array.isArray(t)) fail('tienda debe ser objeto');
      for (const k of Object.keys(t)) if (k !== 'tienda' && k !== 'numero') fail(`clave desconocida "${k}" en tienda`);
      const tienda = String(t.tienda ?? '').trim();
      const numero = String(t.numero ?? '').trim();
      if (tienda.length > 100) fail('tienda máx 100 caracteres');
      if (numero.length > 50) fail('numero máx 50 caracteres');
      if (msTieneHtml(tienda) || msTieneHtml(numero)) fail('caracteres < > no permitidos');
      return { tienda, numero };
    });
  }
  if (v.internacional !== undefined && v.internacional !== null) {
    if (!Array.isArray(v.internacional)) fail('internacional debe ser array');
    if (v.internacional.length > 5) fail('máximo 5 métodos');
    out.internacional = v.internacional.map((m) => {
      if (!m || typeof m !== 'object' || Array.isArray(m)) fail('método debe ser objeto');
      for (const k of Object.keys(m)) if (k !== 'plataforma' && k !== 'dato') fail(`clave desconocida "${k}" en internacional`);
      const plataforma = String(m.plataforma ?? '').trim();
      const dato = String(m.dato ?? '').trim();
      if (plataforma.length > 50) fail('plataforma máx 50 caracteres');
      if (dato.length > 500) fail('dato máx 500 caracteres');
      if (msTieneHtml(plataforma) || msTieneHtml(dato)) fail('caracteres < > no permitidos');
      return { plataforma, dato };
    });
  }
  return out;
}

function validarMsCampo(key, v) {
  const fail = (detalle) => { throw Object.assign(new Error(detalle), { campo: key, detalle }); };
  switch (key) {
    case 'hora': {
      if (v === null || v === undefined) return null;
      const s = String(v).trim();
      if (s === '') return null;
      if (!HHMM_RE.test(s)) fail('formato HH:MM');
      return s;
    }
    case 'lugar': return msTexto(v, 2000, key, fail);
    case 'dress_code': return msTexto(v, 500, key, fail);
    case 'estacionamiento': return msTexto(v, 1000, key, fail);
    case 'plus_one': return msTexto(v, 500, key, fail);
    case 'contacto': return msTexto(v, 100, key, fail);
    case 'hero_img': return msUrl(v, 2000, key, fail);
    case 'video_url': return msUrl(v, 2000, key, fail);
    case 'aviso': return msTexto(v, 300, key, fail);
    case 'rsvp_plazo': return msTexto(v, 100, key, fail);
    case 'cronograma': return validarMsCronograma(v, key, fail);
    case 'faq': return validarMsFaq(v, key, fail);
    case 'timeline': return validarMsTimeline(v, key, fail);
    case 'galeria': return validarMsGaleria(v, key, fail);
    case 'regalos': return validarMsRegalos(v, key, fail);
    default: return null;
  }
}

// Valida el body del PUT: whitelist estricta + reglas por campo.
// Devuelve { campos, publicar } (publicar = true|false|undefined).
// Clave `publicar` es control de estado (draft/publish), NO columna.
function validarMinisitioBody(body) {
  const b = body || {};
  const campos = {};
  let publicar;
  for (const key of Object.keys(b)) {
    if (key === 'publicar') {
      if (typeof b[key] !== 'boolean') throw Object.assign(new Error('publicar debe ser boolean'), { campo: 'publicar', detalle: 'true|false' });
      publicar = b[key];
      continue;
    }
    if (!MS_ALLOWED.includes(key)) {
      throw Object.assign(new Error('campo no permitido'), { campo: key, detalle: null });
    }
    campos[key] = validarMsCampo(key, b[key]);
  }
  if (Object.keys(campos).length === 0 && publicar === undefined) {
    throw Object.assign(new Error('sin campos para actualizar'), { campo: null, detalle: null });
  }
  return { campos, publicar };
}

async function msResolverNovio(slugInput) {
  const r = await pg.query(
    `SELECT n.id, n.slug, n.nombre_novio, n.nombre_novia, n.fecha_boda, n.telefono_novio, n.email, n.estado, n.password_hash
       FROM cn_novios n
       LEFT JOIN cn_minisitio m ON m.novio_id = n.id
      WHERE upper(n.slug) = upper($1) OR lower(m.minisitio_slug) = lower($1)
      ORDER BY (upper(n.slug) = upper($1)) DESC
      LIMIT 1`,
    [String(slugInput || '').trim()]
  );
  return r.rows[0] || null;
}

async function msGetMinisitio(novioId) {
  const r = await pg.query('SELECT * FROM cn_minisitio WHERE novio_id = $1', [novioId]);
  return r.rows[0] || null;
}

// GET /api/minisitio/:slug — público (sin auth). Columnas explícitas: NUNCA PII.
app.get('/api/minisitio/:slug', async (req, res) => {
  try {
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const q = String(req.params.slug || '').trim();
    if (!q) return res.status(404).json({ error: 'Minisitio no encontrado' });
    const r = await pg.query(
      `SELECT m.*, n.slug, n.nombre_novio, n.nombre_novia, n.fecha_boda, n.estado AS estado_cn
         FROM cn_minisitio m JOIN cn_novios n ON n.id = m.novio_id
        WHERE lower(m.minisitio_slug) = lower($1) OR upper(n.slug) = upper($1)
        ORDER BY (lower(m.minisitio_slug) = lower($1)) DESC
        LIMIT 1`,
      [q]
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Minisitio no encontrado' });
    const m = r.rows[0];
    if (m.estado_cn === 'pausada' || m.estado === 'pausada') return res.status(403).json({ error: 'Lista pausada' });
    // draft/publish: el borrador NO es visible para invitados
    if (m.estado === 'borrador') return res.status(404).json({ error: 'Minisitio no publicado' });
    const etag = `"${new Date(m.updated_at).getTime()}"`;
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('ETag', etag);
    if (req.headers['if-none-match'] === etag) return res.status(304).end();
    res.json({
      slug: m.slug,
      minisitio_slug: m.minisitio_slug,
      estado: m.estado,
      nombre_novio: m.nombre_novio,
      nombre_novia: m.nombre_novia,
      fecha_boda: m.fecha_boda,
      hora: m.hora,
      lugar: m.lugar,
      dress_code: m.dress_code,
      estacionamiento: m.estacionamiento,
      plus_one: m.plus_one,
      contacto: m.contacto,
      hero_img: m.hero_img,
      video_url: m.video_url,
      aviso: m.aviso,
      rsvp_plazo: m.rsvp_plazo,
      cronograma: m.cronograma || [],
      faq: m.faq || [],
      timeline: m.timeline || [],
      galeria: m.galeria || [],
      regalos: m.regalos || {},
      updated_at: m.updated_at,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/minisitio/admin/login — reutiliza cn_novios.password_hash (scrypt).
// Rate-limit escalonado 5/min → 15/15min → 60/hora por slug+IP.
app.post('/api/minisitio/admin/login', cnLoginLimiter60, cnLoginLimiter15, cnLoginLimiter, async (req, res) => {
  try {
    const b = req.body || {};
    const slugIn = String(b.slug || '').trim();
    const pass = b.password || '';
    if (!slugIn || !pass) return res.status(400).json({ error: 'slug y password requeridos' });
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const n = await msResolverNovio(slugIn);
    if (!n) return res.status(404).json({ error: 'Lista no encontrada' });
    if (!n.password_hash) return res.status(403).json({ error: 'Esta lista no tiene contraseña configurada' });
    if (!verifyPassword(pass, n.password_hash)) return res.status(401).json({ error: 'Contraseña incorrecta' });
    const m = await msGetMinisitio(n.id);
    res.json({
      ok: true,
      token: makeAdminToken(n.slug, MINISITIO_ADMIN_SECRET),
      slug: n.slug,
      minisitio_slug: m ? m.minisitio_slug : null,
      exp: new Date(Date.now() + 24 * 3600 * 1000).toISOString(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/minisitio/admin/:slug — datos completos para el editor (token).
app.get('/api/minisitio/admin/:slug', requireMinisitioToken, async (req, res) => {
  try {
    const slug = (req.params.slug || '').toUpperCase().trim();
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    const nr = await pg.query('SELECT id, slug, nombre_novio, nombre_novia, fecha_boda, telefono_novio, email, estado FROM cn_novios WHERE slug = $1', [slug]);
    if (nr.rows.length === 0) return res.status(404).json({ error: 'Lista no encontrada' });
    const n = nr.rows[0];
    const m = await msGetMinisitio(n.id);
    res.json({
      slug: n.slug,
      minisitio_slug: m ? m.minisitio_slug : null,
      estado: m ? m.estado : null,
      compartidos: {
        nombre_novio: n.nombre_novio,
        nombre_novia: n.nombre_novia,
        fecha_boda: n.fecha_boda,
        telefono_novio: n.telefono_novio,
        email: n.email,
        estado_cn: n.estado,
      },
      editable: {
        hora: m ? m.hora : null,
        lugar: m ? m.lugar : null,
        dress_code: m ? m.dress_code : null,
        estacionamiento: m ? m.estacionamiento : null,
        plus_one: m ? m.plus_one : null,
        contacto: m ? m.contacto : null,
        hero_img: m ? m.hero_img : null,
        video_url: m ? m.video_url : null,
        aviso: m ? m.aviso : null,
        rsvp_plazo: m ? m.rsvp_plazo : null,
        cronograma: m ? (m.cronograma || []) : [],
        faq: m ? (m.faq || []) : [],
        timeline: m ? (m.timeline || []) : [],
        galeria: m ? (m.galeria || []) : [],
        regalos: m ? (m.regalos || {}) : {},
      },
      updated_at: m ? m.updated_at : null,
      permisos: MS_PERMISOS,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/minisitio/admin/:slug — patch con whitelist, validación por campo,
// snapshot en cn_minisitio_historial (rollback <5 min), upsert, removidos y Slack.
// draft/publish (M3): publicar:true → 'publicada'; publicar:false → 'borrador' explícito.
// Un PUT de contenido SIN publicar PRESERVA el estado actual (editar un sitio
// publicado no lo baja a borrador).
app.put('/api/minisitio/admin/:slug', requireMinisitioToken, async (req, res) => {
  let client = null;
  let fila = null;
  let filaAnterior = null;
  try {
    const slug = (req.params.slug || '').toUpperCase().trim();
    if (!pg) return res.status(500).json({ error: 'Postgres no configurado' });
    let validado;
    try {
      validado = validarMinisitioBody(req.body);
    } catch (e) {
      return res.status(400).json({ error: e.message, campo: e.campo || undefined, detalle: e.detalle || undefined });
    }
    const { campos, publicar } = validado;

    client = await pg.connect();
    try {
      await client.query('BEGIN');
      const nr = await client.query('SELECT id, slug FROM cn_novios WHERE upper(slug) = upper($1)', [slug]);
      if (nr.rows.length === 0) {
        await client.query('ROLLBACK');
        client.release(); client = null;
        return res.status(404).json({ error: 'Lista no encontrada' });
      }
      const novioId = nr.rows[0].id;

      const mr = await client.query('SELECT * FROM cn_minisitio WHERE novio_id = $1 FOR UPDATE', [novioId]);
      filaAnterior = mr.rows[0] || null;

      if (!filaAnterior) {
        // Upsert: fila no existe → INSERT. El seed siempre crea la fila antes de dar
        // acceso al editor; este camino cubre bodas provisionadas por script.
        let minisitioSlug = slug.toLowerCase().replace(/[^a-z0-9-]/g, '-');
        console.log(`⚠️ minisitio: PUT sin fila previa — minisitio_slug derivado '${minisitioSlug}' para ${slug} (confirmar con Alejandro)`);
        const cols = ['novio_id', 'codigo_slug', 'minisitio_slug', 'estado'];
        const vals = [novioId, slug, minisitioSlug, publicar === true ? 'publicada' : 'borrador'];
        for (const k of MS_ALLOWED) if (k in campos) { cols.push(k); vals.push(MS_JSONB_COLS.includes(k) ? JSON.stringify(campos[k]) : campos[k]); }
        const sql = `INSERT INTO cn_minisitio (${cols.join(', ')})
          VALUES (${cols.map((c, i) => `$${i + 1}${MS_JSONB_COLS.includes(c) ? '::jsonb' : ''}`).join(', ')})
          RETURNING *`;
        const ins = await client.query(sql, vals);
        fila = ins.rows[0];
      } else {
        // Snapshot del estado ANTERIOR (excl. id/novio_id) para rollback manual
        const snapshot = { ...filaAnterior };
        delete snapshot.id;
        delete snapshot.novio_id;
        await client.query(
          'INSERT INTO cn_minisitio_historial (novio_id, minisitio_slug, campos_anteriores, campos_nuevos, autor, motivo) VALUES ($1,$2,$3,NULL,$4,$5)',
          [novioId, filaAnterior.minisitio_slug, JSON.stringify(snapshot), 'novio', 'guardar_seccion']
        );
        const updCols = [];
        const updVals = [];
        for (const k of MS_ALLOWED) {
          if (k in campos) {
            updCols.push(`${k} = $${updVals.length + 1}${MS_JSONB_COLS.includes(k) ? '::jsonb' : ''}`);
            // B1 (fix 2026-09-09): node-postgres serializa arrays JS como array literal
            // de PG, no como JSON → '{}'::jsonb violaba el CHECK / invalid input syntax.
            updVals.push(MS_JSONB_COLS.includes(k) ? JSON.stringify(campos[k]) : campos[k]);
          }
        }
        // M3 (fix 2026-08-27): update de contenido NO baja el estado — solo
        // publicar:false explícito (botón "Pasar a borrador") cambia a borrador.
        const nuevoEstado = publicar === true ? 'publicada'
          : (publicar === false ? 'borrador'
            : (filaAnterior.estado || 'borrador'));
        updCols.push(`estado = $${updVals.length + 1}`);
        updVals.push(nuevoEstado);
        updCols.push('updated_at = NOW()');
        const up = await client.query(
          `UPDATE cn_minisitio SET ${updCols.join(', ')} WHERE novio_id = $${updVals.length + 1} RETURNING *`,
          [...updVals, novioId]
        );
        fila = up.rows[0];
      }
      await client.query('COMMIT');
      client.release(); client = null;
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch (e) { /* noop */ }
      throw err;
    }

    // Anti-huérfanos (D1/H7.8): URLs propias de /uploads/{slug}/ reemplazadas o quitadas
    const removidos = [];
    if (fila && filaAnterior) {
      const msSlug = fila.minisitio_slug;
      const esPropia = (url) => {
        try {
          const u = new URL(String(url));
          const parts = u.pathname.split('/');
          return parts.includes('uploads') && parts.includes(msSlug) && MS_UUID_JPG_RE.test(parts[parts.length - 1]);
        } catch (e) { return false; }
      };
      if (filaAnterior.hero_img && campos.hero_img !== undefined && filaAnterior.hero_img !== campos.hero_img && esPropia(filaAnterior.hero_img)) {
        removidos.push(filaAnterior.hero_img);
      }
      if (campos.galeria !== undefined) {
        const viejas = (filaAnterior.galeria || []).map((g) => (typeof g === 'string' ? g : g.url));
        const nuevas = (campos.galeria || []).map((g) => g.url);
        for (const v of viejas) if (!nuevas.includes(v) && esPropia(v)) removidos.push(v);
      }
    }

    const camposCambiados = Object.keys(campos).join(', ');
    try { await notifySlack(`✏️ *Minisitio actualizado* (${slug}): ${camposCambiados || 'solo estado'} — ${new Date().toISOString()}`); } catch (e) { /* noop */ }
    console.log(`✏️ minisitio PUT ${slug}: campos=${camposCambiados || '-'} estado=${fila.estado}`);
    res.json({ ok: true, slug, updated_at: fila.updated_at, estado: fila.estado, removidos });
  } catch (err) {
    if (client) { try { await client.query('ROLLBACK'); } catch (e) { /* noop */ } client.release(); }
    // E1 (fix 2026-09-09): no exponer el detalle crudo de PG al editor — log interno.
    console.error('❌ minisitio PUT error:', err);
    res.status(500).json({ error: 'No se pudo guardar. Reintenta o avísanos al equipo.' });
  }
});

// POST /api/minisitio/admin/validate-token — helper para upload.php/cleanup.php
// (Bluehost no tiene el secret; valida acá y devuelve el código CN resuelto).
app.post('/api/minisitio/admin/validate-token', async (req, res) => {
  try {
    const b = req.body || {};
    const token = String(b.token || '');
    const slugIn = String(b.slug || '').trim();
    if (!token || !slugIn) return res.status(400).json({ error: 'token y slug requeridos' });
    let slug = slugIn.toUpperCase().trim();
    let minisitio_slug = null;
    if (pg) {
      try {
        const r = await pg.query(
          `SELECT n.slug AS codigo, m.minisitio_slug FROM cn_novios n
             LEFT JOIN cn_minisitio m ON m.novio_id = n.id
            WHERE upper(n.slug) = upper($1) OR lower(m.minisitio_slug) = lower($1)
            ORDER BY (upper(n.slug) = upper($1)) DESC
            LIMIT 1`,
          [slugIn.trim()]
        );
        if (r.rows[0]) { slug = r.rows[0].codigo; minisitio_slug = r.rows[0].minisitio_slug || null; }
      } catch (e) { /* noop */ }
    }
    if (!verifyAdminToken(token, slug, MINISITIO_ADMIN_SECRET)) return res.status(401).json({ error: 'No autorizado' });
    res.json({ ok: true, slug, minisitio_slug });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Start ───────────────────────────────────────────────────
async function start() {
  // F1: fail-closed — sin MINISITIO_ADMIN_SECRET el servicio NO arranca (H11)
  if (!MINISITIO_ADMIN_SECRET) {
    console.error('❌ MINISITIO_ADMIN_SECRET no está configurado — abortando (fail closed)');
    process.exit(1);
  }
  try {
    await redis.connect();
    console.log('✅ Redis connected');
  } catch (err) {
    console.warn('⚠️ Redis not available, starting without it');
  }

  await initPostgres();
  await migrateGuestsToListHash(); // F1: lista → hash (compatibilidad con invitados viejos)

  app.listen(PORT, () => {
    console.log(`💒 claw-wedding-agent v1.8.2 running on port ${PORT}`);
    console.log(`   Tenant:          ${TENANT.id}`);
    console.log(`   Health:          http://localhost:${PORT}/status`);
    console.log(`   Webhook WA:      http://localhost:${PORT}/webhook`);
    console.log(`   Webhook Slack:   http://localhost:${PORT}/slack/events`);
    console.log(`   WhatsApp:        ${WHATSAPP_TOKEN ? '✅ configured' : '❌ missing'}`);
    console.log(`   Slack:           ${SLACK_BOT_TOKEN && SLACK_CHANNEL_ID ? '✅ configured' : '❌ missing'}`);
    console.log(`   Slack Events:    ${SLACK_SIGNING_SECRET ? '✅ configured' : '⚠️ not configured (needed for Slack→WA)'}`);
    console.log(`   DeepSeek LLM:   ${DEEPSEEK_API_KEY ? `✅ ${DEEPSEEK_MODEL}` : '⚠️ heuristic fallback'}`);
    console.log(`   Redis:           ${redis.status === 'ready' ? '✅ connected' : '❌ not connected'}`);
    console.log(`   Postgres:        ${pg ? '✅ connected (leads)' : '❌ DATABASE_URL missing'}`);
    console.log(`   Novios:          ${TENANT.noviosPhones.join(', ')}`);
    console.log(`   Simulator:       http://localhost:${PORT}/admin/simulate-webhook`);
  });
}

start().catch(err => {
  console.error('Failed to start:', err);
  process.exit(1);
});

module.exports = { app, sendWhatsAppMessage, sendTemplate, TENANT };
