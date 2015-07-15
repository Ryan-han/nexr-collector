package com.nexr.master.jpa;

import com.nexr.master.CollectorException;

public class JPAExecutorException extends CollectorException {

    public JPAExecutorException() {
        super();
    }

    public JPAExecutorException(String message) {
        super(message);
    }

    public JPAExecutorException(String message, Throwable cause) {
        super(message, cause);
    }

    public JPAExecutorException(Throwable cause) {
        super(cause);
    }
}
