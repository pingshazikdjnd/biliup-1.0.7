import {
  Form,
  Modal,
  Notification,
  Collapse,
  Select,
  Avatar,
} from '@douyinfe/semi-ui'
import { FormApi } from '@douyinfe/semi-ui/lib/es/form'
import React, { useRef } from 'react'
import { useState } from 'react'
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
    // 从 LiveStreamerEntity 接口定义中获取所有字段
    const entityFields = new Set([
      'id',
      'url',
      'remark',
      'filename',
      'split_time',
      'split_size',
      'upload_id',
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

    if (values) {
      const isPlainObject = (val: any): val is Record<string, any> =>
        Boolean(val) && typeof val === 'object' && !Array.isArray(val)

      const nextValues = { ...values }
      const rawOverrideText = nextValues.override_text
      delete nextValues.override_text

      let baseOverride: Record<string, any> = {}
      if (typeof rawOverrideText === 'string' && rawOverrideText.trim() !== '') {
        try {
          const parsed = JSON.parse(rawOverrideText)
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
      Object.keys(nextValues).forEach(key => {
        if (!entityFields.has(key)) {
          const val = nextValues[key]
          if (isPlainObject(val)) {
            const existing = isPlainObject(overrideConfig[key]) ? overrideConfig[key] : {}
            const merged: Record<string, any> = { ...existing }
            Object.entries(val).forEach(([subKey, subVal]) => {
              if (subVal !== undefined && subVal !== null && subVal !== '') {
                merged[subKey] = subVal
              } else {
                delete merged[subKey]
              }
            })
            if (Object.keys(merged).length > 0) {
              overrideConfig[key] = merged
            } else {
              delete overrideConfig[key]
            }
          } else if (val !== undefined && val !== null && val !== '') {
            overrideConfig[key] = val
          } else {
            delete overrideConfig[key]
          }
          delete nextValues[key]
        }
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
      <Form initValues={entity} getFormApi={formApi => (api.current = formApi)}>
        <Form.TextArea
          field="override_text"
          label="配置覆写"
          placeholder="请输入 JSON 格式的配置"
          style={{ marginBottom: 12 }}
          initValue={entity?.override ? JSON.stringify(entity.override, null, 2) : ''}
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
          <Collapse defaultActiveKey={['plugin']}>
            {downloadSettings}
            {(() => {
              const Plugin = platformSetting()
              return Plugin ? (
                <Plugin entity={entity} list={list} initValues={entity?.override} isOverride={true} />
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
