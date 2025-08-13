// Subagent Router - Routes requests to specialized agents
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class SubagentRouter {
  constructor() {
    this.agents = {
      snowflake: {
        path: path.join(__dirname, '../../../packages/agents/snowflake-agent'),
        config: 'ccode.toml'
      },
      audit: {
        path: path.join(__dirname, '../../../packages/agents/audit-agent'),
        config: 'ccode.toml'
      }
    };
  }

  async route(agentName, payload) {
    const agent = this.agents[agentName];
    if (!agent) {
      throw new Error(`Unknown agent: ${agentName}`);
    }

    return new Promise((resolve, reject) => {
      const agentProcess = spawn('ccode', [
        '--config', agent.config,
        '--json', JSON.stringify(payload)
      ], {
        cwd: agent.path,
        stdio: ['pipe', 'pipe', 'pipe']
      });

      let output = '';
      let error = '';

      agentProcess.stdout.on('data', (data) => {
        output += data.toString();
      });

      agentProcess.stderr.on('data', (data) => {
        error += data.toString();
      });

      agentProcess.on('close', (code) => {
        if (code !== 0) {
          reject(new Error(`Agent ${agentName} failed: ${error}`));
        } else {
          try {
            const result = JSON.parse(output);
            resolve(result);
          } catch (e) {
            resolve({ raw: output });
          }
        }
      });

      // Send payload to agent
      agentProcess.stdin.write(JSON.stringify(payload));
      agentProcess.stdin.end();
    });
  }

  async invokeSnowflake(template, params) {
    return this.route('snowflake', {
      type: 'execute_template',
      template,
      params
    });
  }

  async invokeAudit(claim, context) {
    return this.route('audit', {
      type: 'verify_claim',
      claim,
      context
    });
  }
}