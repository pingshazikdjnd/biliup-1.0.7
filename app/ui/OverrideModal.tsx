import {
  Form,
  Modal,
  Notification,
  Collapse,
  Select,
  Avatar,
} from '@douyinfe/semi-ui'
import { FormApi } from '@douyinfe/semi-ui/lib/es/form'
import React, { useEffect, useMemo, useRef, useState } from 'react'
import { LiveStreamerEntity } from '../lib/api-streamer'
import { SupportedPlatforms } from '@/app/ui/plugins'
import { useBiliUsers } from '../lib/use-streamers'

type PluginProps = {
  entity?: LiveStreamerEntity
  list?: { value: string | number; label: React.ReactNode }[]
  initValues?: any
  isOverride?: boolean
}

type TemplateModalProps = {
  visible?: boolean
  entity?: LiveStreamerEntity
  children?: React.ReactNode
  onOk: (e: any) => Promise<void>
}

type OverrideModalContentProps = {
  visible: boolean
  entity?: LiveStreamerEntity
  onOk: (e: any) => Promise<void>
  onCancel: () => void
  afterClose: () => void
}

const removeCircularReferences = (obj: any, seen = new WeakSet()): any => {
  // 处理 null 或非对象类型
  if (obj === null || typeof obj !== 'object') return obj

  // 检测循环引用
  if (seen.has(obj)) return '[Circular Reference]'
  seen.add(obj)

  if (Array.isArray(obj)) {
    return obj.map((item: any) => removeCircularReferences(item, seen))
  }

  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(obj)) {
    // 跳过 React 相关的属性
    if (key === '_context' || key === 'Provider' || key === 'Consumer') continue
    result[key] = removeCircularReferences(value, seen)
  }
  return result
}

// 对标 Python LiveStreamers 表字段与 API DTO 字段；有意排除 filename_prefix，使其保留在 downloadSettings 中写入 override
const ENTITY_FIELDS = new Set([
  'id',
  'url',
  'remark',
  'filename',
  'split_time',
  'split_size',
  'upload_id',
  'upload_streamers_id',
  'status',
  'format',
  'time_range',
  'excluded_keywords',
  'preprocessor',
  'segment_processor',
  'downloaded_processor',
  'postprocessor',
  'opt_args',
  'override',
])

const isPlainObject = (val: any): val is Record<string, any> =>
  Boolean(val) && typeof val === 'object' && !Array.isArray(val)

const valuesEqual = (a: any, b: any): boolean => {
  if (a === b) return true
  if (a == null || b == null || typeof a !== typeof b) return false
  if (typeof a === 'object') {
    try {
      return JSON.stringify(a) === JSON.stringify(b)
    } catch {
      return false
    }
  }
  return false
}

const OverrideModalContent: React.FC<OverrideModalContentProps> = ({
  visible,
  entity,
  onOk,
  onCancel,
  afterClose,
}) => {
  const platformSetting = () => {
    for (const [pattern, Plugin] of Object.entries(SupportedPlatforms)) {
      if (entity?.url.match(new RegExp(pattern))) {
        return Plugin as React.ComponentType<PluginProps>
      }
    }
    return null
  }

  const api = useRef<FormApi>()
  const touchedFieldsRef = useRef<Set<string>>(new Set())
  const touchedUserFieldsRef = useRef<Set<string>>(new Set())
  const isInitializingRef = useRef(true)

  const initialOverrideText = useMemo(
    () => (entity?.override ? JSON.stringify(entity.override, null, 2) : ''),
    [entity?.override]
  )
  const initialOverride = useMemo<Record<string, any>>(() => {
    if (!initialOverrideText) return {}
    try {
      const parsed = JSON.parse(initialOverrideText)
      return isPlainObject(parsed) ? parsed : {}
    } catch {
      return {}
    }
  }, [initialOverrideText])
  const mergedInitValues = useMemo(() => {
    const base: Record<string, any> = entity ? { ...entity } : {}
    Object.entries(initialOverride).forEach(([key, value]) => {
      if (!ENTITY_FIELDS.has(key)) {
        base[key] = value
      }
    })
    return base
  }, [entity, initialOverride])

  useEffect(() => {
    // 子组件 Plugin 的 mount useEffect 先于父组件执行，待其初始 setValue 完成后再开启变更追踪
    isInitializingRef.current = false
  }, [])

  useEffect(() => {
    touchedFieldsRef.current.clear()
    touchedUserFieldsRef.current.clear()
  }, [entity?.id])

  const handleValueChange = (_values: any, changedValue: Record<string, any>) => {
    if (isInitializingRef.current || !changedValue || typeof changedValue !== 'object') {
      return
    }
    if (Object.prototype.hasOwnProperty.call(changedValue, 'override_text')) {
      // 用户修改了 JSON 文本框，重置此前记录的控件触摸状态；之后再次操作控件会重新记入
      touchedFieldsRef.current.clear()
      touchedUserFieldsRef.current.clear()
      return
    }
    Object.entries(changedValue).forEach(([key, val]) => {
      if (key === 'override_text') return
      if (key.startsWith('user.')) {
        const subKey = key.slice('user.'.length)
        if (subKey) {
          touchedUserFieldsRef.current.add(subKey)
        }
        return
      }
      if (key === 'user') {
        if (isPlainObject(val)) {
          Object.keys(val).forEach(subKey => touchedUserFieldsRef.current.add(subKey))
        } else {
          touchedFieldsRef.current.add('user')
        }
        return
      }
      touchedFieldsRef.current.add(key)
    })
  }

  const { biliUsers } = useBiliUsers()
  const list = biliUsers?.map(item => {
    return {
      value: item.value,
      label: (
        <>
          <Avatar size="extra-small" src={item.face} />
          <span style={{ marginLeft: 8 }}>{item.name}</span>
        </>
      ),
    }
  })

  const handleOk = async () => {
    let values = await api.current?.validate()

    if (values) {
      const nextValues = { ...values }
      const rawOverrideText = nextValues.override_text
      delete nextValues.override_text
      const normalizedOverrideText = typeof rawOverrideText === 'string' ? rawOverrideText : ''
      const overrideTextChanged = normalizedOverrideText !== initialOverrideText

      let baseOverride: Record<string, any> = {}
      if (normalizedOverrideText.trim() !== '') {
        try {
          const parsed = JSON.parse(normalizedOverrideText)
          if (!isPlainObject(parsed)) {
            throw new Error('Override must be a JSON object')
          }
          baseOverride = parsed
        } catch (e) {
          Notification.error({
            title: '错误',
            content: '配置格式不正确，请检查 JSON 对象格式',
          })
          return
        }
      }

      const overrideConfig: Record<string, any> = { ...baseOverride }
      const touchedFields = touchedFieldsRef.current
      const touchedUserFields = touchedUserFieldsRef.current

      const candidateKeys = new Set<string>()
      Object.keys(nextValues).forEach(key => {
        if (!ENTITY_FIELDS.has(key)) candidateKeys.add(key)
      })
      touchedFields.forEach(key => {
        if (!ENTITY_FIELDS.has(key)) candidateKeys.add(key)
      })
      if (touchedUserFields.size > 0) {
        candidateKeys.add('user')
      }

      candidateKeys.forEach(key => {
        const val = nextValues[key]
        const origVal = initialOverride[key]
        const isTouchedTop = touchedFields.has(key)

        if (key === 'user') {
          const existingUser = isPlainObject(overrideConfig.user) ? { ...overrideConfig.user } : {}
          const nextUser = isPlainObject(val) ? val : {}
          const origUser = isPlainObject(origVal) ? origVal : {}
          const userSubKeys = new Set<string>(Object.keys(nextUser))
          touchedUserFields.forEach(subKey => userSubKeys.add(subKey))

          userSubKeys.forEach(subKey => {
            const subVal = nextUser[subKey]
            const isTouchedSub = touchedUserFields.has(subKey) || isTouchedTop
            if (overrideTextChanged && !isTouchedSub && valuesEqual(subVal, origUser[subKey])) {
              return
            }
            if (subVal !== undefined && subVal !== null && subVal !== '') {
              existingUser[subKey] = subVal
            } else if (isTouchedSub) {
              delete existingUser[subKey]
            }
          })

          if (Object.keys(existingUser).length > 0) {
            overrideConfig.user = existingUser
          } else {
            delete overrideConfig.user
          }
          delete nextValues.user
          return
        }

        if (isPlainObject(val)) {
          const existing = isPlainObject(overrideConfig[key]) ? { ...overrideConfig[key] } : {}
          const origObj = isPlainObject(origVal) ? origVal : {}
          Object.entries(val).forEach(([subKey, subVal]) => {
            if (overrideTextChanged && !isTouchedTop && valuesEqual(subVal, origObj[subKey])) {
              return
            }
            if (subVal !== undefined && subVal !== null && subVal !== '') {
              existing[subKey] = subVal
            } else if (isTouchedTop) {
              delete existing[subKey]
            }
          })
          if (Object.keys(existing).length > 0) {
            overrideConfig[key] = existing
          } else if (isTouchedTop) {
            delete overrideConfig[key]
          }
        } else if (overrideTextChanged && !isTouchedTop && valuesEqual(val, origVal)) {
          // 用户直接清空或修改了 override_text，未在之后手动修改的表单旧值不再回写覆盖
        } else if (val !== undefined && val !== null && val !== '') {
          overrideConfig[key] = val
        } else if (isTouchedTop) {
          delete overrideConfig[key]
        }
        delete nextValues[key]
      })
      nextValues.override = overrideConfig

      // 处理循环引用
      const cleanValues = removeCircularReferences(nextValues)
      await onOk(cleanValues)
      onCancel()
      return
    }
    onCancel()
  }

  const downloadSettings = (
    <Collapse.Panel header="下载设置" itemKey="download">
      <div style={{ marginBottom: 12 }}>
        请到
        <a href="/dashboard" style={{ textDecoration: 'none', color: 'var(--semi-color-primary)' }}>
          空间配置
        </a>
        查看选项说明
      </div>
      <Form.Select
        label="下载插件（downloader）"
        field="downloader"
        placeholder="stream-gears（默认）"
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
        showClear={true}
      >
        <Select.Option value="streamlink">streamlink（hls多线程下载）</Select.Option>
        <Select.Option value="ffmpeg">ffmpeg</Select.Option>
        <Select.Option value="stream-gears">stream-gears（默认）</Select.Option>
        <Select.Option value="sync-downloader">sync-downloader（边录边传）</Select.Option>
        <Select.Option value="mesio">mesio（FLV/HLS 时间戳修复）</Select.Option>
      </Form.Select>

      <Form.InputNumber
        label="视频分段大小（file_size）"
        field="file_size"
        placeholder=""
        suffix={'Byte'}
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
      />

      <Form.Input
        field="segment_time"
        label="视频分段时长（segment_time）"
        placeholder="01:00:00"
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
        showClear={true}
        rules={[
          {
            pattern: /^[^：]*$/,
            message: '请使用英文冒号',
          },
          {
            pattern: /^[0-9:]*$/,
            message: '只接受数字和英文冒号',
          },
          {
            pattern: /^[0-9]{2,4}:[0-5][0-9]:[0-5][0-9]$/,
            message: '分或秒不符合规范',
          },
        ]}
        stopValidateWithError={true}
      />

      <Form.Input
        field="filename_prefix"
        label="文件名模板（filename_prefix）"
        placeholder="{streamer}%Y-%m-%dT%H_%M_%S"
      />

      <Form.InputNumber
        field="filtering_threshold"
        label="碎片过滤（filtering_threshold）"
        suffix={'MB'}
        style={{ width: '100%' }}
        fieldStyle={{
          alignSelf: 'stretch',
          padding: 0,
        }}
        showClear={true}
      />
    </Collapse.Panel>
  )

  return (
    <Modal
      title="配置覆写"
      visible={visible}
      onOk={handleOk}
      style={{ width: 'min(600px, 90vw)' }}
      onCancel={onCancel}
      afterClose={afterClose}
      bodyStyle={{
        overflow: 'auto',
        maxHeight: 'calc(100vh - 320px)',
        paddingLeft: 10,
        paddingRight: 10,
      }}
    >
      <Form
        initValues={mergedInitValues}
        getFormApi={formApi => (api.current = formApi)}
        onValueChange={handleValueChange}
      >
        <Form.TextArea
          field="override_text"
          label="配置覆写"
          placeholder="请输入 JSON 格式的配置"
          style={{ marginBottom: 12 }}
          initValue={initialOverrideText}
          rules={[
            { required: false },
            {
              validator: (rule, value) => {
                if (!value) return true
                try {
                  JSON.parse(value)
                  return true
                } catch (e) {
                  return false
                }
              },
              message: '请输入有效的 JSON 格式',
            },
          ]}
        />
        <Form.Section>
          <Collapse keepDOM defaultActiveKey={['plugin']}>
            {downloadSettings}
            {(() => {
              const Plugin = platformSetting()
              return Plugin ? (
                <Plugin
                  entity={entity}
                  list={list}
                  initValues={initialOverride}
                  isOverride={true}
                />
              ) : null
            })()}
          </Collapse>
        </Form.Section>
      </Form>
    </Modal>
  )
}

const OverrideModal: React.FC<TemplateModalProps> = ({ children, entity, onOk }) => {
  const [mounted, setMounted] = useState(false)
  const [visible, setVisible] = useState(false)
  const showDialog = () => {
    setMounted(true)
    setVisible(true)
  }
  const handleCancel = () => {
    setVisible(false)
  }
  const handleAfterClose = () => {
    setMounted(false)
  }

  const childrenWithProps = React.Children.map(children, child => {
    if (React.isValidElement<any>(child)) {
      return React.cloneElement(child, {
        onClick: () => {
          showDialog()
          child.props.onClick?.()
        },
      })
    }
  })

  return (
    <>
      {childrenWithProps}
      {mounted ? (
        <OverrideModalContent
          visible={visible}
          entity={entity}
          onOk={onOk}
          onCancel={handleCancel}
          afterClose={handleAfterClose}
        />
      ) : null}
    </>
  )
}

export default OverrideModal
