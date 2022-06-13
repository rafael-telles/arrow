package org.apache.arrow.flight;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import javax.annotation.Nullable;

public class RefreshClientInterceptor implements ClientInterceptor {
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                             io.grpc.CallOptions callOptions, Channel next) {
    System.out.println("Intercept the call");
    return new RetryClientCall<>(callOptions, next, method);
  }

  static class RetryClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

    Listener listener;
    Metadata metadata;
    CallOptions callOptions;
    Channel next;
    int req;
    ReqT msg;
    ClientCall call;
    MethodDescriptor method;

    public RetryClientCall(CallOptions callOptions, Channel next, MethodDescriptor method) {
      this.callOptions = callOptions;
      this.next = next;
      this.method = method;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      this.listener = responseListener;
      this.metadata = headers;

      startCall(new CheckingListener());


      System.out.println("Run start method from interceptor");
    }



    @Override
    public void request(int numMessages) {
      System.out.println("Run request method from interceptor");
      req += numMessages;
      call.request(numMessages);

    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      System.out.println("Run cancel method from interceptor");
    }

    @Override
    public void halfClose() {
      System.out.println("Run halfClose method from interceptor");
      call.halfClose();
    }

    private void startCall(Listener listener) {
      System.out.println("Run startCall method from interceptor");

      call = next.newCall(method, callOptions);
      Metadata headers = new Metadata();
      headers.merge(metadata);
      call.start(listener, headers);
    }

    @Override
    public void sendMessage(ReqT message) {
      assert this.msg == null;
      this.msg = message;
      call.sendMessage(msg);
    }

    class CheckingListener extends ForwardingClientCallListener {
      Listener<RespT> delegate;

      @Override
      protected Listener delegate() {
        if (delegate == null) {
          throw new IllegalStateException();
        }
        return delegate;
      }

      @Override
      public void onReady() {
        listener.onReady();
      }

      @Override
      public void onHeaders(Metadata headers) {
        delegate = listener;
        super.onHeaders(headers);
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        System.out.println("Run close method from listener interceptor");
        if (delegate != null) {
          super.onClose(status, trailers);
          return;
        }
        if (!needToRetry(status, trailers)) { // YOUR CODE HERE
          delegate = listener;
          super.onClose(status, trailers);
          return;
        }
         start(listener, trailers); // to allow multiple retries
      }
    }

    private boolean needToRetry(Status status, Metadata trailers) {
      return status.getCode().toStatus() == Status.UNAUTHENTICATED;
    }
  }
}
