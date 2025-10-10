# Telcos → HubSpot (Claro + Tigo) — Cron & API

Pipeline para:
- **Subir contactos y mensajes** de **Claro** y **Tigo** a HubSpot.
- **Asociar** mensajes (custom object) con contactos por número.
- **Auto-crear contactos** para **mensajes huérfanos** y asociarlos.
- Ejecutar **crons diarios** y correr jobs **manuales** vía HTTP.

---

## 1) Requisitos

- Node.js 18+ (recomendado 20+)
- Token de HubSpot (Private App)
- Credenciales de Claro Notifícame
- Credenciales de Tigo B2B (Apigee)
- Acceso para crear propiedades/objetos/asociaciones en tu portal HubSpot

---

## 2) Configuración

Crea `.env` desde el ejemplo y completa tus valores:

```bash
cp .env.example .env
```

Variables clave (resumen):
- `HUBSPOT_TOKEN`: token Private App.
- `HUBSPOT_CONTACTS_OBJECT`: normalmente `0-1`.
- `HUBSPOT_MESSAGES_OBJECT`: FQN o `objectTypeId` (p.ej. `2-50592224`).
- `HUBSPOT_MESSAGES_ID_PROPERTY`: propiedad única del objeto mensajes, p.ej. `id_mensaje_unico`.
- `HUBSPOT_ID_PROPERTY`: propiedad única de contactos, p.ej. `numero_telefono_id_unico`.
- `HUBSPOT_ASSOC_LABEL_MSG_TO_CONTACT`: **label** de la asociación mensaje→contacto (p.ej. `Contactos`).
- `CLARO_*` y `TIGO_*`: credenciales y parámetros de fuente.
- Producción: `SYNC_DEDUPE=1`, `SEND_TO_HUBSPOT=true`.

---

## 3) Instalación

```bash
npm install
```

---

## 4) Scripts principales

### Servidor + crons

```bash
npm run serve
```
- Healthcheck: `GET http://localhost:3000/health`
- Cron interno:
  - **Claro** (contactos + mensajes): 08:00 GT
  - **Tigo** (mensajes + contactos): 09:00 GT
- Endpoints manuales:
  - `GET /cron/claro/run` → corre Claro ahora (contactos + mensajes)
  - `GET /cron/tigo/run`  → corre Tigo ahora (mensajes + contactos + asociaciones)

### Jobs manuales (CLI)

```bash
# Tigo (solo mensajes, paginado correcto)
npm run tigo:mensajes

# Tigo (solo contactos)
npm run tigo:contactos

# Asociar mensajes (p_mensajes) → contactos por número (usa label del .env)
npm run assoc:tigo

# Diagnóstico de asociaciones (rápido / con CSV)
npm run check:assoc
npm run assoc:report

# Diagnóstico de teléfonos
npm run diag:phones

# Reparar huérfanos: crea contactos faltantes (compañía) y asocia
npm run orphans:fix
```

### Mensajes/Contactos Claro (si quieres correrlos sueltos)

```bash
# Contactos Claro
node src/jobs/sync.js

# Mensajes Claro
node src/jobs/sync_messages.js
```

> O usa el endpoint `/cron/claro/run` que corre **ambos**.

---

## 5) Flujo de datos

1) **Claro**
- `src/jobs/sync.js`  
  - Genera contactos desde msisdn → normaliza a **E.164** y **upsert** (id: `numero_telefono_id_unico`).
  - Incluye `compania="Claro"`.
- `src/jobs/sync_messages.js`  
  - Sube mensajes → objeto `HUBSPOT_MESSAGES_OBJECT` (prop única `id_mensaje_unico`).
  - Incluye `numero` (E.164), `contenido`, `estado`, `fecha`.
  - **No** asocia (para eso están los jobs de asociación).

2) **Tigo**
- `src/jobs/sync_tigo_messages.js`  
  - Paginación correcta (`?page=1..N&size=500`). Upsert al objeto de **mensajes**.
- `src/jobs/sync_tigo_contacts.js`  
  - Upsert de **contactos** (id: `numero_telefono_id_unico`) con `compania="Tigo"`.

3) **Asociaciones**
- `src/jobs/associate_tigo_messages_contacts.js`  
  - Lee mensajes (`numero`), busca contacto por `numero_telefono_id_unico` y **crea asociación** con el **label** del `.env` (`HUBSPOT_ASSOC_LABEL_MSG_TO_CONTACT`).
- `src/jobs/fix_orphan_messages_contacts.js`  
  - Si falta contacto, **lo crea** con `firstname/lastname` por defecto y `compania` inferida (Tigo/Claro) y asocia.

---

## 6) Verificación

- **Contar mensajes en HubSpot**: Objeto `Mensajes` → vista lista → total.
- **Ver asociaciones**:
  - `npm run assoc:report` → genera CSV en `reports/` con filas `messageId,contactId,[typeIds]`.
  - `npm run check:assoc` (modo “fast”): cuenta cuántos mensajes tienen asociaciones.
- **Buscar por número**:
  - Contactos: buscar `numero_telefono_id_unico` (formato `+502XXXXXXXX`).
  - Mensajes: filtrar por propiedad `numero` (si la vista lo muestra).  
    Si la búsqueda de UI es limitada, usa los scripts de reporte.

---

## 7) Logs/Debug

- `DEBUG_SYNC=1` → logs de mapeo, dedupe, batches.
- `SYNC_LOG_EACH=1` → log por registro (ruidoso).
- `DEBUG_ASSOC=1` → imprime labels disponibles y `typeId` elegido.
- `diag:phones` → cuántos números coinciden entre mensajes y contactos.

---

## 8) Producción (PM2 sugerido)

```bash
# instalar pm2
npm i -g pm2

# levantar
pm2 start src/server.js --name telcos-hs --env production

# logs
pm2 logs telcos-hs

# persistir en reinicio
pm2 startup
pm2 save
```

Asegúrate de que el `.env` esté junto al proyecto (o exporta variables en tu servicio).

---

## 9) Errores comunes

- **INVALID_AUTHENTICATION**: revisa `HUBSPOT_TOKEN` (scopes y vigencia).
- **PROPERTY_DOESNT_EXIST**: crea la propiedad en HubSpot con **el nombre interno exacto**.
- **idProperty no única**: confirma que la propiedad de upsert tiene `hasUniqueValue=true`.
- **No aparecen asociaciones**: verifica el **label** en `HUBSPOT_ASSOC_LABEL_MSG_TO_CONTACT` (ej. `Contactos`) y que los teléfonos estén en **E.164** en **ambos** objetos.
- **Mensajes duplicados**: en prod usa `SYNC_DEDUPE=1` y cuida la generación de `id_mensaje_unico`.

---

## 10) Estructura útil (El resto de archivos fueron como pruebas y no afectan ni son importantes para el funcionamiento)

```
src/
  jobs/
    sync.js                      # Contactos Claro (compania="Claro")
    sync_messages.js             # Mensajes Claro
    sync_tigo_contacts.js        # Contactos Tigo (compania="Tigo")
    sync_tigo_messages.js        # Mensajes Tigo (paginado)
    associate_tigo_messages_contacts.js
    fix_orphan_messages_contacts.js
  providers/
    claro.js                     # SDK/firmas para Claro
    tigo.js                      # Paginación & helpers Tigo
  sinks/
    hubspotContacts.js           # Upsert contactos
    hubspotCustom.js             # Upsert custom objects (mensajes)
    hubspotAssoc.js              # Resolver typeIds + batch asociaciones
  tools/
    check-assoc.js               # Conteo rápido de asociaciones
    print-associated-pairs.js    # Reporte CSV de pares
scripts/
  print-assoc-types.js           # Listar labels (typeIds) disponibles

