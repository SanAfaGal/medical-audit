# Migrar producción a otra máquina

Runbook para mover el stack completo (base de datos + PDFs) de un servidor de producción a otro —
cambio de hardware, migración a la nube, etc. Para backups periódicos sin cambiar de máquina, ver
la sección **Backups** en [docs/DEPLOY.md](DEPLOY.md).

Lo único que hay que transportar son los dos volúmenes con estado: `medical-audit-prod_pgdata`
(vía dump lógico, no copia cruda de archivos) y `medical-audit-prod_audit_data` (PDFs). Todo lo
demás (código, imágenes Docker, configuración) se reconstruye desde el repo en la máquina destino.

---

## 1. En la máquina de origen: generar los backups

```bash
./deploy.sh backup-full
# → backups/full_TIMESTAMP.dump (dump lógico de PostgreSQL, formato --custom)
```

```bash
mkdir -p backups
docker run --rm \
  -v medical-audit-prod_audit_data:/data:ro \
  -v "$(pwd)/backups":/backups \
  alpine tar czf /backups/audit_data_$(date +%Y%m%d_%H%M%S).tar.gz -C /data .
```

El tar puede pesar varios GB (todos los PDFs auditados) y tarda varios minutos. Confirmar que
terminó antes de seguir: el proceso libera el contenedor temporal (`--rm`) al finalizar, y el
tamaño del archivo deja de crecer.

> Si el servidor de origen es Windows, los comandos son los mismos — Docker Desktop traduce las
> rutas automáticamente. Solo cambiar `$(pwd)/backups` por la ruta absoluta si el shell no la
> resuelve (ej. `C:\ruta\al\proyecto\backups`).

## 2. Transferir los dos archivos a la máquina destino

Cualquier método de copia sirve (scp, disco externo, sync a un bucket). Vía `scp` desde la máquina
destino (pull), asumiendo acceso SSH a la máquina origen:

```bash
scp usuario@origen:/ruta/proyecto/backups/full_TIMESTAMP.dump ./backups/
scp usuario@origen:/ruta/proyecto/backups/audit_data_TIMESTAMP.tar.gz ./backups/
```

**Gotchas de SSH con Windows como origen o destino** (encontrados migrando este proyecto):

- Autenticación por password no se puede automatizar sin `sshpass`/sin herramienta equivalente —
  el cliente OpenSSH lee el password directo de la tty, no acepta stdin. Para dejar de escribir el
  password repetidas veces, agregar una clave pública a `authorized_keys` es más simple.
- En el **OpenSSH Server de Windows**, `authorized_keys` debe estar en
  `C:\Users\<usuario>\.ssh\authorized_keys` y **pertenecer al mismo usuario que intenta loguearse**.
  Si el archivo lo crea otra cuenta (ej. un usuario admin escribiendo en la carpeta de otro usuario),
  sshd lo ignora silenciosamente por el chequeo de permisos (falla igual que si la clave no existiera,
  sin mensaje de error específico) — hay que crear el archivo logueado como el usuario dueño de esa
  clave, o apuntar la clave a la cuenta que sí lo creó.
- No pegar passwords de producción en canales que quedan registrados (chats, tickets). Si ya pasó,
  rotar la credencial después de terminar la migración.

## 3. En la máquina destino: levantar el stack vacío

```bash
git clone <repo-url>
cd medical-audit-v2
cp .env.example .env
# editar .env con las credenciales de producción (ver docs/DEPLOY.md paso 2)

./deploy.sh up
```

Esto construye las imágenes, aplica las migraciones de Alembic sobre una base vacía y corre el
seed inicial. El seed se sobrescribe en el paso siguiente al restaurar el dump completo — no importa
que corra.

Verificar que los tres contenedores están sanos antes de continuar:

```bash
./deploy.sh ps
```

## 4. Restaurar la base de datos

```bash
./deploy.sh restore-full backups/full_TIMESTAMP.dump
```

Detiene el `backend` antes de restaurar (evita escrituras concurrentes) y lo reinicia al terminar.
Esto reemplaza por completo los datos del seed inicial por los datos reales de producción.

## 5. Restaurar el volumen de PDFs

```bash
docker run --rm \
  -v medical-audit-prod_audit_data:/data \
  -v "$(pwd)/backups":/backups \
  alpine tar xzf /backups/audit_data_TIMESTAMP.tar.gz -C /data
```

## 6. Verificar

```bash
./deploy.sh health
curl http://localhost/health/db
```

Abrir la aplicación en el navegador y confirmar que aparecen las instituciones, períodos y facturas
esperadas (no solo el seed vacío). Revisar que los PDFs abren correctamente desde el explorador —
confirma que el volumen `audit_data` quedó bien restaurado, no solo la base de datos.

## 7. Apagar la máquina de origen

Solo después de confirmar el paso 6. No borrar el volumen/base de datos original hasta tener la
migración verificada en la máquina nueva por al menos unos días.

```bash
# Actualizar firewall / DNS / lo que apunte a la IP vieja antes de apagarla
```

---

## Referencia rápida

| Qué se copia | Cómo se genera | Cómo se restaura |
|---|---|---|
| Base de datos | `./deploy.sh backup-full` | `./deploy.sh restore-full <archivo>` |
| PDFs (`audit_data`) | `docker run ... tar czf ...` (ver paso 1) | `docker run ... tar xzf ...` (ver paso 5) |

Detalle completo de cada comando de backup/restore individual: [docs/DEPLOY.md § Backups](DEPLOY.md#backups).
