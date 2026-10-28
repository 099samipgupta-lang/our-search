-- OUR SEARCH: AI submission and approval foundation

create table if not exists public.ai_submissions (
    id uuid primary key default gen_random_uuid(),

    creator_id uuid not null
        references auth.users(id) on delete cascade,

    name text not null
        check (char_length(name) between 2 and 80),

    description text not null default ''
        check (char_length(description) <= 2000),

    category text not null default 'other'
        check (char_length(category) <= 50),

    submission_type text not null
        check (submission_type in ('model_file', 'api_connection')),

    model_storage_path text,
    api_provider text,

    status text not null default 'pending_review'
        check (status in (
            'draft',
            'pending_review',
            'changes_requested',
            'rejected',
            'approved',
            'published'
        )),

    review_note text,
    reviewed_at timestamptz,
    published_at timestamptz,

    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint model_submission_has_path check (
        submission_type <> 'model_file'
        or model_storage_path is not null
        or status = 'draft'
    ),

    constraint api_submission_has_provider check (
        submission_type <> 'api_connection'
        or api_provider is not null
        or status = 'draft'
    )
);

create index if not exists ai_submissions_creator_idx
    on public.ai_submissions (creator_id, created_at desc);

create index if not exists ai_submissions_published_idx
    on public.ai_submissions (published_at desc)
    where status = 'published';

alter table public.ai_submissions enable row level security;

drop policy if exists "Creators can view own AI submissions"
    on public.ai_submissions;

create policy "Creators can view own AI submissions"
on public.ai_submissions
for select
to authenticated
using ((select auth.uid()) = creator_id);

drop policy if exists "Anyone can view published AI submissions"
    on public.ai_submissions;

create policy "Anyone can view published AI submissions"
on public.ai_submissions
for select
to anon, authenticated
using (status = 'published');

drop policy if exists "Creators can submit their own AI"
    on public.ai_submissions;

create policy "Creators can submit their own AI"
on public.ai_submissions
for insert
to authenticated
with check (
    (select auth.uid()) = creator_id
    and status in ('draft', 'pending_review')
);

-- Creators can edit submission details, but cannot update status,
-- review notes, reviewer timestamps, or publication timestamps.
revoke all on public.ai_submissions from anon, authenticated;

grant select on public.ai_submissions to anon, authenticated;

grant insert (
    creator_id,
    name,
    description,
    category,
    submission_type,
    model_storage_path,
    api_provider,
    status
) on public.ai_submissions to authenticated;

grant update (
    name,
    description,
    category,
    model_storage_path,
    api_provider,
    updated_at
) on public.ai_submissions to authenticated;

