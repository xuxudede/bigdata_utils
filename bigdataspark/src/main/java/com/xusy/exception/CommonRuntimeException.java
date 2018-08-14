package com.xusy.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommonRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -6922197809457025559L;

    private static final Logger logger = LoggerFactory.getLogger(CommonRuntimeException.class);

    public CommonRuntimeException(Throwable cause, String msg) {
        super(msg, cause);
    }

    public CommonRuntimeException(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);

        checkArgs(args);
    }

    public CommonRuntimeException(String msg) {
        super(msg);
    }

    public CommonRuntimeException(String format, Object... args) {
        super(String.format(format, args));

        checkArgs(args);
    }

    private void checkArgs (Object[] args) {
        if (args.length > 0) {
            for (Object arg : args) {
                if (arg instanceof Throwable) {
                    logger.error("请注意改正: CommonRuntimeException 参数 和 JDK.Exception不一致: ", (Throwable) arg);
                }
            }
        }
    }

}
