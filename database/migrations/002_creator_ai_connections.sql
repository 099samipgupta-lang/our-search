-- ============================================================
-- OUR SEARCH: Secure creator AI API connections
-- ============================================================

CREATE TABLE IF NOT EXISTS public.ai_creator_connections (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    creator_id uuid NOT NULL
        REFERENCES auth.users(id) ON DELETE CASCADE,

    submission_id uuid
        REFERENCES public.ai_submissions(id) ON DELETE SET NULL,

    provider text NOT NULL,
    endpoint_url text NOT NULL,
    api_format text NOT NULL DEFAULT 'openai',
    model text NOT NULL,

    auth_type text NOT NULL
        CHECK (auth_type IN ('bearer', 'header', 'none')),

    header_name text,

    -- Encrypted by the OUR SEARCH backend before insertion.
    encrypted_api_key text NOT NULL DEFAULT '',

    -- SHA-256 hash of a random, temporary test token.
    test_token_hash text UNIQUE,

    status text NOT NULL DEFAULT 'staged'
        CHECK (status IN ('staged', 'active')),

    expires_at timestamptz,

    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS
    ai_creator_connections_creator_idx
ON public.ai_creator_connections(creator_id);

CREATE INDEX IF NOT EXISTS
    ai_creator_connections_status_expiry_idx
ON public.ai_creator_connections(status, expires_at);

ALTER TABLE public.ai_creator_connections ENABLE ROW LEVEL SECURITY;

-- No direct access from browser-facing roles.
REVOKE ALL ON public.ai_creator_connections
FROM PUBLIC, anon, authenticated;

GRANT ALL ON public.ai_creator_connections TO service_role;

-- ============================================================
-- Atomically convert a tested connection into an AI submission.
-- The backend supplies the authenticated creator ID and
-- SHA-256 hash of the one-time test token.
-- ============================================================

CREATE OR REPLACE FUNCTION public.save_creator_ai_connection(
    p_test_token_hash text,
    p_creator_id uuid,
    p_name text,
    p_description text,
    p_category text
)
RETURNS TABLE (
    saved_submission_id uuid,
    saved_connection_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
    v_connection public.ai_creator_connections%ROWTYPE;
    v_submission_id uuid;
BEGIN
    IF p_test_token_hash IS NULL
       OR length(p_test_token_hash) <> 64
       OR p_creator_id IS NULL
       OR length(trim(p_name)) = 0
       OR length(trim(p_description)) = 0
       OR length(trim(p_category)) = 0
    THEN
        RAISE EXCEPTION 'Invalid submission details';
    END IF;

    SELECT *
    INTO v_connection
    FROM public.ai_creator_connections
    WHERE test_token_hash = p_test_token_hash
      AND creator_id = p_creator_id
      AND status = 'staged'
      AND expires_at > now()
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Connection test expired or invalid';
    END IF;

    INSERT INTO public.ai_submissions (
        creator_id,
        name,
        description,
        category,
        submission_type,
        api_provider,
        status
    )
    VALUES (
        p_creator_id,
        trim(p_name),
        trim(p_description),
        trim(p_category),
        'api_connection',
        v_connection.provider,
        'pending_review'
    )
    RETURNING id INTO v_submission_id;

    UPDATE public.ai_creator_connections
    SET submission_id = v_submission_id,
        status = 'active',
        test_token_hash = NULL,
        expires_at = NULL,
        updated_at = now()
    WHERE id = v_connection.id;

    RETURN QUERY
    SELECT v_submission_id, v_connection.id;
END;
$$;

REVOKE ALL ON FUNCTION public.save_creator_ai_connection(
    text, uuid, text, text, text
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.save_creator_ai_connection(
    text, uuid, text, text, text
) TO service_role;
