package com.lgc.dspdm.core.common.util;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Try<T> {

    private Try() {
    }

    public static <T> Try<T> apply(FailableSupplier<T> supplier) {
        try {
            return new Success<>(supplier.get());
        } catch (Throwable e) {
            if (e instanceof Exception) return new Failure(e);
            else throw ((Error) e);
        }
    }

    @SuppressWarnings("unchecked")
    static <T extends Throwable, R> R sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

    public abstract T get();

    public abstract boolean isSuccess();

    public abstract void forEach(Consumer<? super T> action);

    public abstract <U> Try<U> map(Function<? super T, ? extends U> mapper);

    public abstract <U> Try<U> flatMap(Function<? super T, ? extends Try<U>> mapper);

    public abstract Try<T> filter(Predicate<? super T> predicate);

    public abstract Optional<T> toOptional();

    public abstract T getOrElse(T defaultValue);

    public final Try<T> onSuccess(Consumer<? super T> action) {
        Objects.requireNonNull(action, "action is null");
        if (isSuccess()) {
            action.accept(get());
        }
        return this;
    }

    @FunctionalInterface
    public interface FailableSupplier<T> {

        /**
         * @return a value of type {@code T}
         * @throws Throwable if it fails
         */
        T get() throws Throwable;
    }

    public static final class Success<T> extends Try<T> {

        private final T value;

        public Success(T value) {
            this.value = value;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public void forEach(Consumer<? super T> action) {

        }

        @Override
        public <U> Try<U> map(Function<? super T, ? extends U> mapper) {
            return null;
        }

        @Override
        public <U> Try<U> flatMap(Function<? super T, ? extends Try<U>> mapper) {
            return null;
        }

        @Override
        public Try<T> filter(Predicate<? super T> predicate) {
            return null;
        }

        @Override
        public Optional<T> toOptional() {
            return Optional.empty();
        }

        @Override
        public T getOrElse(T defaultValue) {
            return value;
        }

    }

    public static final class Failure<T> extends Try<T> {

        private final Throwable exception;

        public Failure(Throwable exception) {
            this.exception = exception;
        }


        @Override
        public T get() {
            return sneakyThrow(exception);
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public void forEach(Consumer<? super T> action) {

        }

        @Override
        public <U> Try<U> map(Function<? super T, ? extends U> mapper) {
            return null;
        }

        @Override
        public <U> Try<U> flatMap(Function<? super T, ? extends Try<U>> mapper) {
            return null;
        }

        @Override
        public Try<T> filter(Predicate<? super T> predicate) {
            return null;
        }

        @Override
        public Optional<T> toOptional() {
            return Optional.empty();
        }

        @Override
        public T getOrElse(T defaultValue) {
            return defaultValue;
        }
    }
}
