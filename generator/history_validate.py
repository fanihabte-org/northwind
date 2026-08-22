"""Read-only validation and reporting for inferred lifecycle history."""
from __future__ import annotations
import json, os
import psycopg

OPS_DSN=os.getenv("OPS_PG_DSN","postgresql://ops:ops@localhost:5433/ops")
CHECKS={
 "orders":("ops.orders","ops.order_status_history","order_id"),
 "shipments":("ops.shipments","ops.shipment_status_history","shipment_id"),
 "invoices":("ops.invoices","ops.invoice_status_history","invoice_id"),
 "support_cases":("ops.support_cases","ops.support_case_status_history","case_id"),
}
def report(connection):
 out={}
 with connection.cursor() as c:
  for name,(source,history,key) in CHECKS.items():
   c.execute(f"SELECT count(*) FROM {source}"); eligible=c.fetchone()[0]
   c.execute(f"SELECT count(DISTINCT {key}) FROM {history} WHERE anomaly_type='inferred_baseline'"); inferred=c.fetchone()[0]
   c.execute(f"SELECT count(DISTINCT {key}) FROM {history} WHERE anomaly_type IS DISTINCT FROM 'inferred_baseline'"); existing=c.fetchone()[0]
   c.execute(f"SELECT count(*) FROM (SELECT {key}, min(occurred_at)>max(recorded_at) AS bad FROM {history} GROUP BY {key}) x WHERE bad"); invalid=c.fetchone()[0]
   out[name]={"eligible":eligible,"inferred":inferred,"skipped_existing_history":existing,"invalid_history":invalid}
 return out
def main():
 with psycopg.connect(OPS_DSN) as conn: print(json.dumps(report(conn),indent=2))
if __name__=='__main__': main()
