module.exports = async (req, res) => {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "method_not_allowed" });
  }

  const githubWorkflowToken = process.env.GITHUB_WORKFLOW_TOKEN;
  const expectedRepo = process.env.GITHUB_REPOSITORY || "jykim4846/etf_check";
  const workflowFile = process.env.GITHUB_WORKFLOW_FILE || "daily.yml";

  if (!githubWorkflowToken) {
    return res.status(500).json({ error: "missing_server_secrets" });
  }

  const response = await fetch(
    `https://api.github.com/repos/${expectedRepo}/actions/workflows/${workflowFile}/runs?per_page=1`,
    {
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${githubWorkflowToken}`,
        "User-Agent": "etf-check-manual-trigger",
        "X-GitHub-Api-Version": "2022-11-28",
      },
    },
  );

  if (!response.ok) {
    const errorText = await response.text();
    return res.status(response.status).json({
      error: "github_run_lookup_failed",
      detail: errorText,
    });
  }

  const payload = await response.json();
  const run = Array.isArray(payload.workflow_runs) ? payload.workflow_runs[0] : null;
  if (!run) {
    return res.status(200).json({
      ok: true,
      run: null,
      actions_url: `https://github.com/${expectedRepo}/actions/workflows/${workflowFile}`,
    });
  }

  return res.status(200).json({
    ok: true,
    actions_url: `https://github.com/${expectedRepo}/actions/workflows/${workflowFile}`,
    run: {
      id: run.id,
      html_url: run.html_url,
      status: run.status,
      conclusion: run.conclusion,
      event: run.event,
      created_at: run.created_at,
      updated_at: run.updated_at,
      head_branch: run.head_branch,
      head_sha: run.head_sha,
      display_title: run.display_title,
    },
  });
};
