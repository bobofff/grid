// PM2 process definition for the grid runner.
//
// The defaults are wrong for this app in two places, and both of them would show
// up as a live grid with nobody watching it:
//
//   kill_timeout - PM2 sends the stop signal, then force-kills with SIGKILL 1600ms
//                  later.  A risk exit has to cancel every resting order and then
//                  flatten, which can take tens of seconds.  At the default,
//                  risk cleanup can be cut in half.
//
//   autorestart  - recoverable faults and PM2 restarts resume from persisted
//                  clientOrderIds and reconcile against Binance.  ROI risk exits
//                  persist STOPPED:<reason>, which refuses automatic re-entry.
//
//   restart backoff - a Binance/network outage must not cause a tight restart
//                     loop. PM2 grows the delay from five seconds after each
//                     consecutive short-lived failure, then stops after ten.
//
//   kill_signal  - PM2 defaults to SIGINT, which lands in Python as
//                  KeyboardInterrupt at an arbitrary bytecode boundary.  SIGTERM
//                  goes through install_signal_handlers instead, so the runner
//                  stops at a safe point and the journal records the signal name.
//                  If your PM2 build ignores `kill_signal`, set the environment
//                  variable PM2_KILL_SIGNAL=SIGTERM instead.
//
// Usage:
//   pm2 start ecosystem.config.js
//   pm2 logs grid-bot
//   pm2 logs grid-dashboard
//   pm2 stop grid-bot
//   pm2 stop grid-dashboard
//   pm2 delete grid-bot
//   pm2 delete grid-dashboard
module.exports = {
  apps: [
    {
      name: 'grid-bot',
      cwd: __dirname,

      // `python -m grid_bot.main`, not `grid_bot/main.py`: the package uses
      // relative imports, so it has to be imported as a module.
      script: '.venv/bin/python',
      args: '-m grid_bot.main',
      interpreter: 'none',

      exec_mode: 'fork',
      instances: 1,

      autorestart: true,
      // A deliberate, fail-closed pause (including Binance 429/418) exits with
      // code 0. Keep it stopped until an operator has reviewed the exchange.
      stop_exit_codes: [0],
      exp_backoff_restart_delay: 5000,
      min_uptime: 60000,
      max_restarts: 10,
      kill_timeout: 90000,
      kill_signal: 'SIGTERM',

      // Without this, Python block-buffers stdout when it is not a TTY and PM2's
      // log file lags behind reality.
      env: { PYTHONUNBUFFERED: '1' },

      out_file: './runtime/pm2-out.log',
      error_file: './runtime/pm2-error.log',
      merge_logs: true,
      time: true,
    },
    {
      // The local-only browser console is independent from the runner. Keeping it
      // separate lets operators restart the UI without touching active orders.
      name: 'grid-dashboard',
      cwd: __dirname,
      script: '.venv/bin/python',
      args: '-m grid_bot.dashboard',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,
      autorestart: true,
      kill_timeout: 5000,
      kill_signal: 'SIGTERM',
      env: { PYTHONUNBUFFERED: '1' },
      out_file: './runtime/dashboard-pm2-out.log',
      error_file: './runtime/dashboard-pm2-error.log',
      merge_logs: true,
      time: true,
    },
  ],
};
