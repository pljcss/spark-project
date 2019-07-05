package utils;

import com.scala.test.Test2;
import org.apache.commons.cli.*;

public class CLITest {
    public static void main(String[] args) throws ParseException {

        //输入参数
        String[] arg = {"-f","xx.file"};

        //解析参数
        CommandLine cmdLine = parseArg(arg);

        //拿到重要参数
        System.out.println(cmdLine.getOptionValue("f"));
        //TODO
    }

    private static CommandLine parseArg(String[] args) throws ParseException {
        // 定义阶段
        Options options = new Options();
        options.addOption("h", false, "usage help");
        options.addOption("help", false, "usage help");
        options.addOption("f", true, "configuration file");
        options.addOption("l", true, "log file");
        options.addOption("w", true, "filter worker number");
        options.addOption("v", false, "print info log");
        options.addOption("vv", false, "print debug log");
        options.addOption("vvvv", false, "print trace log");

        // 解析阶段
        CommandLineParser paraer = new PosixParser();
        CommandLine cmdLine = paraer.parse(options, args);

        // 询问阶段
        if (cmdLine.hasOption("help") || cmdLine.hasOption("h")) {
            /*usage(); //这里作者自定义了帮助信息，其实可以使用helpFormat直接输出的*/

            HelpFormatter hf = new HelpFormatter();
            hf.setWidth(110);
            hf.printHelp("testApp", options, true);

            System.exit(-1);
        }

        // TODO need process invalid arguments
        if (!cmdLine.hasOption("f")) {
            throw new IllegalArgumentException("Required -f argument to specify config file");
        }

        return cmdLine;
    }
}
