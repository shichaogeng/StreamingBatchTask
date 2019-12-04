package com.xueqiu.streaming.batch.task.core.bo;


import cn.hutool.core.util.StrUtil;
import com.xueqiu.streaming.batch.task.common.ClassUtil;
import lombok.Data;
import lombok.NonNull;


import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Data
public class TaskSettingWrapper {

    private static final String SETTING_FIELD_DEFAULT = "id";

    private Class cls;
    private Class<? extends Annotation> annotationCls;
    private List<String> returnTypeNames;
    private int methodParameterCount;
    private String customFieldName;

    private Method method;
    private Field field;

    public TaskSettingWrapper(@NonNull Class cls,
                              Class<? extends Annotation> annotationCls,
                              List<String> returnTypeNames) {
        this(cls, annotationCls, returnTypeNames, 0);
    }

    public TaskSettingWrapper(@NonNull Class cls,
                              Class<? extends Annotation> annotationCls,
                              List<String> returnTypeNames,
                              int methodParameterCount) {
        this(cls, annotationCls, returnTypeNames, methodParameterCount, null);
    }

    public TaskSettingWrapper(@NonNull Class cls,
                              Class<? extends Annotation> annotationCls,
                              List<String> returnTypeNames,
                              int methodParameterCount,
                              String customFieldName) {
        this.cls = cls;
        this.annotationCls = annotationCls;
        this.returnTypeNames = returnTypeNames == null ? new ArrayList<>() : returnTypeNames;
        this.methodParameterCount = methodParameterCount;
        this.customFieldName = customFieldName;
        // 初始化
        init();
    }

    public void init() {
        // 尝试获取注解方法（只用第一个使用该注解的方法）
        this.method = ClassUtil.getAnnotatedDeclaredMethodList(cls, annotationCls)
                .stream()
                .filter(m -> m.getParameterCount() == this.methodParameterCount)
                .filter(m -> returnTypeNames.contains(m.getGenericReturnType().getTypeName()))
                .findAny().orElse(null);
        if (method != null) {
            return;
        }
        // 尝试从注解属性中获取
        Field[] fields = ClassUtil.getAnnotatedDeclaredFields(cls, annotationCls, false);
        if (fields != null && fields.length != 0) {
            this.field = fields[0];
            return;
        }
        String compareField = StrUtil.isNotBlank(this.customFieldName) ? this.customFieldName : SETTING_FIELD_DEFAULT;
        // 尝试从默认属性id中获取
        fields = ClassUtil.getDeclaredFields(cls, false);
        for (Field f : fields) {
            if (f.getName().equals(compareField)) {
                this.field = f;
                break;
            }
        }
    }

    public <T> T getSettingInfo(Object o, Class<T> resultType) {
        // 尝试从注解方法中获取（只用第一个使用该注解的方法）
        if (this.method != null) {
            try {
                return resultType.cast(method.invoke(o));
            } catch (Exception e) {
                return null;
            }
        }
        // 尝试从注解属性中获取
        if (this.field != null) {
            try {
                return resultType.cast(ClassUtil.getFieldValue(field.getName(), o));
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    public boolean isUsable() {
        return this.method != null || this.field != null;
    }
}
