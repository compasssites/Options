# Claude Instructions

<!-- compass:start:claude -->
Read these first:

@AGENTS.md

AGENTS.md is the single in-repo source: it already contains the global, security,
stack, MCP, project and workflow sections. Do not add imports for .agent/*.md —
those were byte-for-byte duplicates and loaded the same rules twice per session.

Enforcement lives in .claude/settings.json (permissions.deny / permissions.ask and
the Compass PreToolUse hook), compiled from ~/CompassAgentMemory/memory/60-policy.json.
<!-- compass:end:claude -->
