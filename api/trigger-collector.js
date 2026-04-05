module.exports = async (req, res) => {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "method_not_allowed" });
  }

  const manualTriggerToken = process.env.MANUAL_TRIGGER_TOKEN;
  const githubWorkflowToken = process.env.GITHUB_WORKFLOW_TOKEN;
  const expectedRepo = process.env.GITHUB_REPOSITORY || "jykim4846/etf_check";
  const workflowFile = process.env.GITHUB_WORKFLOW_FILE || "daily.yml";
  const workflowRef = process.env.GITHUB_WORKFLOW_REF || "main";

  if (!manualTriggerToken || !githubWorkflowToken) {
    return res.status(500).json({ error: "missing_server_secrets" });
  }

  const providedToken = req.body && typeof req.body.triggerToken === "string"
    ? req.body.triggerToken.trim()
    : "";

  if (!providedToken || providedToken !== manualTriggerToken) {
    return res.status(401).json({ error: "invalid_trigger_token" });
  }

  const dispatchResponse = await fetch(
    `https://api.github.com/repos/${expectedRepo}/actions/workflows/${workflowFile}/dispatches`,
    {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${githubWorkflowToken}`,
        "Content-Type": "application/json",
        "User-Agent": "etf-check-manual-trigger",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({ ref: workflowRef }),
    },
  );

  if (!dispatchResponse.ok) {
    const errorText = await dispatchResponse.text();
    return res.status(dispatchResponse.status).json({
      error: "github_dispatch_failed",
      detail: errorText,
    });
  }

  return res.status(200).json({
    ok: true,
    message: "workflow_dispatch queued",
    repo: expectedRepo,
    workflow: workflowFile,
    ref: workflowRef,
  });
};
