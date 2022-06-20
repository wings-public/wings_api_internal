const { createLogger, format, transports } = require('winston');
const path = require('path');
const fs = require('fs');

const logDir = 'log';
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
}

console.log("********* REquest for logger ********* ");
var pid = process.pid;
var logFile = `logger-track-${pid}.log`;
//var path1 = path.parse(__dirname).dir;
var path1 = __dirname;
const filename = path.join(path1,logDir,logFile);
console.log("filename is "+filename);
const env = 'development';

const logger = createLogger({
        // change level if in dev environment versus production
        level: env === 'development' ? 'debug' : 'info',
        format: format.combine(
            format.timestamp({
                format: 'YYYY-MM-DD HH:mm:ss'
            }),
            format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
        ),
        transports: [
            new transports.Console({
                level: 'info',
                format: format.combine(
                    format.colorize(),
                    format.printf(
                        info => `${info.timestamp} ${info.level}: ${info.message}`
                    )
                )
            }),
            new transports.File({ 
                  filename : filename,
                  format : format.combine(
                      format.json()
                  ) 
            })
        ]
    });

module.exports = {logger};
