/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 回溯批次与容量汇总的逐笔对账行。 */
export type ReconcileLine = {
  任务编号: string
  目标特征: string
  确认行数: number
  已入账: boolean
  配置编号: string
  回执编号: string
  入账行数: number
  差异: number
}

export type ReconcileResult = {
  ok: boolean
  lines: ReconcileLine[]
  summary: {
    回溯批次笔数: number
    回执笔数: number
    回溯确认行数汇总: number
    容量汇总行数: number
    逐笔相符: boolean
  }
}
