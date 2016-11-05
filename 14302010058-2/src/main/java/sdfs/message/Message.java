package sdfs.message;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by shiyuhong on 16/10/23.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private Object[] params;
    private Class[] parameterTypes;
    private Object result;  //这是存储服务端的计算结果的
    private String methodName;



    public Object[] getParams() {
        return params;
    }

    public void setParams(Object[] params) {
        this.params = params;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Class[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }
}
