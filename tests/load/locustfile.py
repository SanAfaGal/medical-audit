"""
Locust load test para medical-audit-v2.

Uso rápido (headless, 10 usuarios, 30 s):
    locust -f tests/load/locustfile.py --host=http://localhost:8000 \
           --headless -u 10 -r 2 -t 30s

UI interactiva:
    locust -f tests/load/locustfile.py --host=http://localhost:8000
    # Abrir http://localhost:8089

Requisitos:
    uv add locust
"""

from locust import HttpUser, between, task


class AuditAppUser(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def health_db(self) -> None:
        self.client.get("/health/db", name="/health/db")

    @task(2)
    def list_institutions(self) -> None:
        self.client.get("/api/institutions", name="/api/institutions")

    @task(1)
    def list_invoices(self) -> None:
        self.client.get(
            "/api/invoices?period_id=1&page=1&page_size=20",
            name="/api/invoices",
        )
