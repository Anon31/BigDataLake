import type { Plugin } from "@opencode-ai/plugin"

const exportToS3: Plugin = async ({ directory, $ }) => {
  return {
    event: async ({ event }) => {
      if (!event || event.type !== "session.idle") return

      const sessionID = event.properties.sessionID
      if (!sessionID) return

      try {
        await $`python3.10 ${directory}/scripts/log_to_parquet.py --session ${sessionID}`
      } catch (err) {
        console.error(`[export-to-s3] échec de l'export de la session ${sessionID}:`, err)
      }
    },
  }
}

export default exportToS3