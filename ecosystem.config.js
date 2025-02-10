module.exports = {
	apps: [
		{
			name: "v2",
			script: "./fetch.py",
			interpreter: "python",
			args: "",
			watch: false,
			instances: 1,
			exec_mode: "fork",
			autorestart: true,
			max_memory_restart: "4096M",
			env: {
				NODE_ENV: "development",
			},
			env_production: {
				NODE_ENV: "production",
			},
			log_date_format: "YYYY-MM-DD HH:mm:ss",
			error_file: "log/err.log",
			out_file: "log/out.log",
			merge_logs: true,
		},
	],
};

pm2LogRotate = {
	module: "pm2-logrotate",
	config: {
		maxSize: "8M",
		retain: 8,
		compress: false,
	},
};
