-- Northwind Systems :: ERP source database, migration 005
-- Posted revenue journal entries are append-only. Financial corrections must
-- be represented by a new CRN or ADJ entry with its own audit timestamp.

CREATE OR REPLACE FUNCTION erp.reject_revenue_posting_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'erp.revenue_postings is append-only; create a CRN or ADJ posting instead'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER tr_revenue_postings_append_only
BEFORE UPDATE OR DELETE ON erp.revenue_postings
FOR EACH ROW EXECUTE FUNCTION erp.reject_revenue_posting_mutation();
