import tkinter as tk
from tkinter import ttk, messagebox
import json
import os
import requests
import time
import threading
import schedule
from datetime import datetime, timedelta

CONFIG_FILE = 'sync_config.json'

def save_config_json(data):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_config_json():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

class XiaoeAPI:
    def __init__(self, app_id, client_id, secret):
        self.app_id = app_id
        self.client_id = client_id
        self.secret = secret
        self.token = None

    def get_token(self):
        url = "https://api.xiaoe-tech.com/token"
        params = {
            "app_id": self.app_id,
            "client_id": self.client_id,
            "secret_key": self.secret,
            "grant_type": "client_credential"
        }
        try:
            res = requests.get(url, params=params).json()
            if res.get('code') == 0:
                self.token = res['data']['access_token']
                return self.token
            raise Exception(res.get('msg'))
        except Exception as e:
            raise Exception(f"小鹅通 Token 获取失败: {str(e)}")

    def get_orders(self, page_size=100, created_time_start=None, created_time_end=None):
        if not self.token: self.get_token()
        url = "https://api.xiaoe-tech.com/xe.ecommerce.order.list/1.0.0"
        
        all_orders = []
        page = 1
        max_pages = 100
        max_retries = 3
        request_delay = 0.5
        
        while page <= max_pages:
            payload = {
                "access_token": self.token,
                "page_size": min(page_size, 100),
                "page": page
            }
            if created_time_start:
                payload["created_time_start"] = created_time_start
            if created_time_end:
                payload["created_time_end"] = created_time_end
            
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    res = requests.post(url, json=payload, timeout=30).json()
                    
                    if res.get('code') == 0:
                        data = res.get('data', {})
                        current_list = []
                        if isinstance(data, dict):
                            if 'list' in data:
                                current_list = data['list']
                            else:
                                current_list = [data] if data else []
                        elif isinstance(data, list):
                            current_list = data
                        
                        if not current_list:
                            if page == 1:
                                print(f"DEBUG: 2.0接口请求成功但无数据. Payload: {json.dumps(payload)}")
                            return all_orders
                            
                        all_orders.extend(current_list)
                        print(f"DEBUG: 2.0接口获取到 {len(current_list)} 条订单 (Page: {page}, 总计: {len(all_orders)})")
                        
                        if len(current_list) < payload['page_size']:
                            return all_orders
                        
                        page += 1
                        success = True
                        time.sleep(request_delay)
                        
                    else:
                        error_code = res.get('code')
                        error_msg = res.get('msg', '')
                        
                        if error_code in [40001, 40002, 40003]:
                            print(f"Token失效，正在重新获取...")
                            self.token = None
                            self.get_token()
                            payload["access_token"] = self.token
                            retry_count += 1
                            time.sleep(1)
                        elif error_code == -1 or 'timeout' in error_msg.lower():
                            print(f"请求超时或服务异常，第 {retry_count + 1} 次重试...")
                            retry_count += 1
                            time.sleep(2 ** retry_count)
                        else:
                            old_url = "https://api.xiaoe-tech.com/xe.order.list.get/1.0.0"
                            res_old = requests.post(old_url, json=payload, timeout=30).json()
                            if res_old.get('code') == 0:
                                data = res_old.get('data', {})
                                current_list = data if isinstance(data, list) else data.get('list', [])
                                if not current_list:
                                    return all_orders
                                all_orders.extend(current_list)
                                print(f"DEBUG: 旧接口获取到 {len(current_list)} 条订单 (Page: {page})")
                                if len(current_list) < payload['page_size']:
                                    return all_orders
                                page += 1
                                success = True
                                time.sleep(request_delay)
                            else:
                                raise Exception(f"接口返回错误: {error_msg} (代码: {error_code})")
                                
                except requests.exceptions.Timeout:
                    print(f"请求超时，第 {retry_count + 1} 次重试...")
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                except requests.exceptions.RequestException as e:
                    print(f"网络请求异常: {str(e)}，第 {retry_count + 1} 次重试...")
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                except Exception as e:
                    if all_orders:
                        print(f"已获取 {len(all_orders)} 条订单，但遇到错误: {str(e)}")
                        return all_orders
                    raise Exception(f"获取订单失败: {str(e)}")
            
            if not success:
                print(f"第 {page} 页获取失败，已重试 {max_retries} 次")
                if all_orders:
                    return all_orders
                raise Exception(f"获取订单失败，已重试 {max_retries} 次")
        
        print(f"已达到最大页数限制 {max_pages} 页，总计获取 {len(all_orders)} 条订单")
        return all_orders

# --- 飞书 API ---
class FeishuAPI:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = None

    def get_token(self):
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        try:
            res = requests.post(url, json={"app_id": self.app_id, "app_secret": self.app_secret}).json()
            if res.get('code') == 0:
                self.token = res['tenant_access_token']
                return self.token
            raise Exception(res.get('msg'))
        except Exception as e:
            raise Exception(f"飞书 Token 获取失败: {str(e)}")

    def get_table_fields(self, app_token, table_id):
        if not self.token: self.get_token()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        all_fields = []
        page_token = ""
        while True:
            params = {"page_size": 100, "page_token": page_token}
            try:
                res = requests.get(url, headers=headers, params=params).json()
                if res.get('code') == 0:
                    data = res.get('data', {})
                    items = data.get('items', [])
                    all_fields.extend([f['field_name'] for f in items])
                    if not data.get('has_more'):
                        break
                    page_token = data.get('page_token')
                else:
                    break
            except:
                break
        return all_fields

    def create_field(self, app_token, table_id, field_name):
        if not self.token: self.get_token()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {"field_name": field_name, "type": 1} # 默认文本
        try:
            res = requests.post(url, json=payload, headers=headers).json()
            return res.get('code') == 0
        except:
            return False

    def batch_create_records(self, app_token, table_id, records):
        if not self.token: self.get_token()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        batch_size = 500
        total = len(records)
        success_count = 0
        
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            try:
                res = requests.post(url, json={"records": batch}, headers=headers, timeout=30).json()
                if res.get('code') == 0:
                    success_count += len(batch)
                    print(f"飞书批量创建进度: {min(i + batch_size, total)}/{total}")
                else:
                    print(f"飞书批量创建失败: {res.get('msg')}")
                    return False
                time.sleep(0.2)
            except Exception as e:
                print(f"飞书批量创建异常: {str(e)}")
                return False
        
        return success_count == total

    def batch_update_records(self, app_token, table_id, records):
        if not self.token: self.get_token()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        batch_size = 500
        total = len(records)
        success_count = 0
        
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            try:
                res = requests.post(url, json={"records": batch}, headers=headers, timeout=30).json()
                if res.get('code') == 0:
                    success_count += len(batch)
                    print(f"飞书批量更新进度: {min(i + batch_size, total)}/{total}")
                else:
                    print(f"飞书批量更新失败: {res.get('msg')}")
                    return False
                time.sleep(0.2)
            except Exception as e:
                print(f"飞书批量更新异常: {str(e)}")
                return False
        
        return success_count == total

    def list_all_records(self, app_token, table_id):
        if not self.token: self.get_token()
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        all_records = []
        page_token = ""
        while True:
            params = {"page_size": 100, "page_token": page_token}
            res = requests.get(url, headers=headers, params=params).json()
            if res.get('code') != 0: break
            data = res.get('data', {})
            all_records.extend(data.get('items', []))
            if not data.get('has_more'): break
            page_token = data.get('page_token')
        return all_records

class SyncApp:
    def __init__(self, root):
        self.root = root
        self.root.title("小鹅通-飞书多维表全功能同步工具")
        self.root.geometry("800x800")
        
        self.mapping_rows = []
        self.xiaoe_fields_list = []
        self.setup_ui()
        self.load_initial_config()
        
        self.is_syncing = False

    def setup_ui(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.config_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.config_frame, text=" 基础配置 ")
        self.setup_config_tab()

        self.mapping_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.mapping_frame, text=" 字段映射 ")
        self.setup_mapping_tab()

        self.log_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.log_frame, text=" 运行日志 ")
        self.setup_log_tab()

    def setup_config_tab(self):
        ttk.Label(self.config_frame, text="小鹅通 API 配置", font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        self.inputs = {}
        for label, key in [("App ID", "xiaoe_app_id"), ("Client ID", "xiaoe_client_id"), ("Secret Key", "xiaoe_app_secret")]:
            f = ttk.Frame(self.config_frame)
            f.pack(fill=tk.X, pady=2)
            ttk.Label(f, text=label, width=15).pack(side=tk.LEFT)
            self.inputs[key] = ttk.Entry(f)
            self.inputs[key].pack(side=tk.LEFT, fill=tk.X, expand=True)

        ttk.Label(self.config_frame, text="飞书多维表配置", font=('Arial', 10, 'bold')).pack(anchor=tk.W, pady=(20, 0))
        for label, key in [("App ID", "feishu_app_id"), ("App Secret", "feishu_app_secret"), 
                           ("App Token", "feishu_app_token"), ("Table ID", "feishu_table_id")]:
            f = ttk.Frame(self.config_frame)
            f.pack(fill=tk.X, pady=2)
            ttk.Label(f, text=label, width=15).pack(side=tk.LEFT)
            self.inputs[key] = ttk.Entry(f)
            self.inputs[key].pack(side=tk.LEFT, fill=tk.X, expand=True)

        ttk.Label(self.config_frame, text="同步设置", font=('Arial', 10, 'bold')).pack(anchor=tk.W, pady=(20, 0))
        
        # 同步天数设置
        f_days = ttk.Frame(self.config_frame)
        f_days.pack(fill=tk.X, pady=2)
        ttk.Label(f_days, text="同步最近天数", width=15).pack(side=tk.LEFT)
        self.inputs['sync_days'] = ttk.Entry(f_days)
        self.inputs['sync_days'].pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.inputs['sync_days'].insert(0, "7") # 默认7天

        # 自定义时间范围
        ttk.Label(self.config_frame, text="自定义同步时间 (可选，格式: 2023-01-01 00:00:00)", foreground="gray").pack(anchor=tk.W, pady=(10, 0))
        f_start = ttk.Frame(self.config_frame)
        f_start.pack(fill=tk.X, pady=2)
        ttk.Label(f_start, text="开始时间", width=15).pack(side=tk.LEFT)
        self.inputs['custom_start'] = ttk.Entry(f_start)
        self.inputs['custom_start'].pack(side=tk.LEFT, fill=tk.X, expand=True)

        f_end = ttk.Frame(self.config_frame)
        f_end.pack(fill=tk.X, pady=2)
        ttk.Label(f_end, text="结束时间", width=15).pack(side=tk.LEFT)
        self.inputs['custom_end'] = ttk.Entry(f_end)
        self.inputs['custom_end'].pack(side=tk.LEFT, fill=tk.X, expand=True)

        f = ttk.Frame(self.config_frame)
        f.pack(fill=tk.X, pady=2)
        ttk.Label(f, text="同步间隔(分钟)", width=15).pack(side=tk.LEFT)
        self.inputs['sync_interval'] = ttk.Entry(f)
        self.inputs['sync_interval'].pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Button(self.config_frame, text="保存所有配置", command=self.save_all).pack(pady=20)

    def setup_mapping_tab(self):
        top_bar = ttk.Frame(self.mapping_frame)
        top_bar.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(top_bar, text="获取小鹅通最新字段", command=self.fetch_xiaoe_fields).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_bar, text="一键映射所有字段", command=self.auto_map_all_fields).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_bar, text="添加单行映射", command=self.add_mapping_row).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_bar, text="清空所有映射", command=self.clear_all_mappings).pack(side=tk.LEFT, padx=5)

        # 滚动区域
        self.canvas = tk.Canvas(self.mapping_frame)
        self.scrollbar = ttk.Scrollbar(self.mapping_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

    def setup_log_tab(self):
        btn_f = ttk.Frame(self.log_frame)
        btn_f.pack(fill=tk.X, pady=(0, 10))
        
        self.sync_btn = ttk.Button(btn_f, text="开始自动同步", command=self.toggle_sync)
        self.sync_btn.pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_f, text="立即同步一次", command=self.sync_once).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_f, text="清空日志", command=lambda: self.log_text.delete(1.0, tk.END)).pack(side=tk.LEFT, padx=5)

        self.log_text = tk.Text(self.log_frame, height=30)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def log(self, msg):
        now = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{now}] {msg}\n")
        self.log_text.see(tk.END)

    def add_mapping_row(self, xiaoe_val="", feishu_val=""):
        row_f = ttk.Frame(self.scrollable_frame)
        row_f.pack(fill=tk.X, pady=2)
        
        x_cb = ttk.Combobox(row_f, values=self.xiaoe_fields_list, width=30)
        x_cb.set(xiaoe_val)
        x_cb.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row_f, text=" -> ").pack(side=tk.LEFT)
        
        f_ent = ttk.Entry(row_f, width=30)
        f_ent.insert(0, feishu_val)
        f_ent.pack(side=tk.LEFT, padx=5)
        
        del_btn = ttk.Button(row_f, text="删除", width=5, command=lambda: self.remove_mapping_row(row_f))
        del_btn.pack(side=tk.LEFT, padx=5)
        
        self.mapping_rows.append({'frame': row_f, 'xiaoe': x_cb, 'feishu': f_ent})

    def remove_mapping_row(self, frame):
        for i, row in enumerate(self.mapping_rows):
            if row['frame'] == frame:
                frame.destroy()
                self.mapping_rows.pop(i)
                break

    def fetch_xiaoe_fields(self):
        threading.Thread(target=self._do_fetch_fields, daemon=True).start()

    def _do_fetch_fields(self):
        try:
            config = {k: v.get() for k, v in self.inputs.items()}
            if not config['xiaoe_app_id'] or not config['xiaoe_client_id'] or not config['xiaoe_app_secret']:
                messagebox.showwarning("提示", "请先填写完整的小鹅通配置")
                return

            api = XiaoeAPI(config['xiaoe_app_id'], config['xiaoe_client_id'], config['xiaoe_app_secret'])
            
            self.log("正在尝试获取小鹅通订单以解析字段...")
            # 仅获取1条数据用于解析字段，避免过长等待
            orders = api.get_orders(page_size=1)
            
            if not orders:
                self.log("提示: 接口请求成功，但您的店铺目前没有订单数据。")
                messagebox.showwarning("提示", "未获取到订单数据：您的店铺当前可能没有任何订单，请手动添加映射行或先产生一笔测试订单。")
                return
            
            sample = orders[0]
            fields = []
            
            def flatten(obj, prefix='', visited=None):
                if visited is None:
                    visited = set()
                
                obj_id = id(obj)
                if obj_id in visited:
                    return
                visited.add(obj_id)
                
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        path = f"{prefix}.{k}" if prefix else k
                        flatten(v, path, visited)
                elif isinstance(obj, list):
                    if not obj:
                        if prefix and prefix not in fields:
                            fields.append(prefix)
                    else:
                        max_items = min(len(obj), 5)
                        for i in range(max_items):
                            item = obj[i]
                            if isinstance(item, dict):
                                for k, v in item.items():
                                    path = f"{prefix}[].{k}" if prefix else k
                                    flatten(v, path, visited)
                            elif isinstance(item, list):
                                flatten(item, f"{prefix}[]", visited)
                            else:
                                path = f"{prefix}[]"
                                if path not in fields:
                                    fields.append(path)
                        if len(obj) > max_items:
                            for k, v in obj[0].items() if isinstance(obj[0], dict) else []:
                                path = f"{prefix}[].{k}" if prefix else k
                                if path not in fields:
                                    fields.append(path)
                else:
                    if prefix and prefix not in fields:
                        fields.append(prefix)
            
            flatten(sample)
            
            self.xiaoe_fields_list = sorted(list(set(fields)))
            
            # 回到主线程更新 UI
            self.root.after(0, self._update_fields_ui)
            
        except Exception as e:
            self.log(f"获取字段失败: {str(e)}")
            self.root.after(0, lambda: messagebox.showerror("错误", f"获取字段失败:\n{str(e)}"))

    def _update_fields_ui(self):
        # 强制更新现有所有 Combobox 的 values
        for row in self.mapping_rows:
            row['xiaoe'].config(values=self.xiaoe_fields_list)
        
        self.log(f"成功解析出 {len(self.xiaoe_fields_list)} 个字段: {', '.join(self.xiaoe_fields_list[:10])}...")
        messagebox.showinfo("成功", f"解析成功！已发现 {len(self.xiaoe_fields_list)} 个字段。\n\n您现在可以：\n1. 点击'一键映射所有字段'自动填入\n2. 或点击'添加单行映射'手动选择")


    def auto_map_all_fields(self):
        if not self.xiaoe_fields_list:
            messagebox.showwarning("提示", "请先点击'获取小鹅通最新字段'")
            return
        
        if messagebox.askyesno("确认", f"将自动添加 {len(self.xiaoe_fields_list)} 行映射，是否继续？"):
            for field in self.xiaoe_fields_list:
                # 飞书字段名直接使用小鹅通字段名（点号换成下划线，方便看）
                feishu_name = field.replace('.', '_')
                self.add_mapping_row(field, feishu_name)
            self.log(f"已自动添加 {len(self.xiaoe_fields_list)} 个映射关系")

    def clear_all_mappings(self):
        if messagebox.askyesno("确认", "确定要清空所有已配置的映射吗？"):
            for row in self.mapping_rows[:]:
                self.remove_mapping_row(row['frame'])
            self.log("已清空所有映射")

    def get_mapping_data(self):
        return [{'xiaoe': r['xiaoe'].get(), 'feishu': r['feishu'].get()} for r in self.mapping_rows if r['xiaoe'].get()]

    def save_all(self):
        data = {k: v.get() for k, v in self.inputs.items()}
        data['field_mapping'] = self.get_mapping_data()
        save_config_json(data)
        messagebox.showinfo("成功", "所有配置及映射已保存")

    def load_initial_config(self):
        config = load_config_json()
        for k, v in config.items():
            if k in self.inputs:
                self.inputs[k].insert(0, str(v))
        
        if 'field_mapping' in config:
            for m in config['field_mapping']:
                self.add_mapping_row(m.get('xiaoe'), m.get('feishu'))

    def sync_once(self):
        threading.Thread(target=self._do_sync, daemon=True).start()

    def _do_sync(self):
        self.log("开始同步流程...")
        try:
            current_mapping = self.get_mapping_data()
            
            if not current_mapping:
                config = load_config_json()
                current_mapping = config.get('field_mapping', [])
            
            if not current_mapping:
                self.log("错误: 未配置字段映射。请先在'字段映射'页添加字段并点击'保存所有配置'。")
                messagebox.showwarning("提示", "请先在'字段映射'页添加字段并点击'保存所有配置'")
                return

            config = load_config_json()
            if not config.get('xiaoe_app_id') or not config.get('feishu_app_id'):
                self.log("错误: 基础配置不完整")
                return

            xiaoe = XiaoeAPI(config['xiaoe_app_id'], config['xiaoe_client_id'], config['xiaoe_app_secret'])
            feishu = FeishuAPI(config['feishu_app_id'], config['feishu_app_secret'])
            
            self.log("检查飞书多维表结构...")
            existing_f_fields = feishu.get_table_fields(config['feishu_app_token'], config['feishu_table_id'])
            existing_set = {f.strip() for f in existing_f_fields}
            
            created_in_this_run = set()
            for m in current_mapping:
                f_name = m['feishu'].strip()
                if f_name and f_name not in existing_set and f_name not in created_in_this_run:
                    self.log(f"自动创建缺失字段: {f_name}")
                    if feishu.create_field(config['feishu_app_token'], config['feishu_table_id'], f_name):
                        created_in_this_run.add(f_name)
                    else:
                        self.log(f"警告: 字段 '{f_name}' 创建失败")

            custom_start = config.get('custom_start', '').strip()
            custom_end = config.get('custom_end', '').strip()
            
            if custom_start:
                start_time = custom_start
                end_time = custom_end if custom_end else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.log(f"使用自定义时间范围: {start_time} 至 {end_time}")
            else:
                sync_days = int(config.get('sync_days', 7))
                start_time = (datetime.now() - timedelta(days=sync_days)).strftime("%Y-%m-%d %H:%M:%S")
                end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.log(f"获取最近 {sync_days} 天的订单 (自 {start_time} 以来)...")
            
            orders = xiaoe.get_orders(page_size=100, created_time_start=start_time, created_time_end=end_time)
            if not orders:
                self.log(f"未发现可同步订单 (时间范围: {start_time} 至 {end_time})")
                return
            
            self.log(f"成功获取到 {len(orders)} 条订单数据，准备同步...")

            self.log("拉取飞书现有记录进行比对...")
            existing_records = feishu.list_all_records(config['feishu_app_token'], config['feishu_table_id'])
            
            unique_feishu_col = None
            unique_paths = ['order_info.order_id', 'order_id', 'id']
            for path in unique_paths:
                for m in current_mapping:
                    if m['xiaoe'] == path:
                        unique_feishu_col = m['feishu']
                        break
                if unique_feishu_col: break
            
            if not unique_feishu_col:
                unique_feishu_col = current_mapping[0]['feishu']
                self.log(f"警告: 未发现 order_id 映射，将使用 '{unique_feishu_col}' 作为重复检查标识")

            feishu_id_map = {}
            for item in existing_records:
                u_id = str(item.get('fields', {}).get(unique_feishu_col, ''))
                if u_id:
                    feishu_id_map[u_id] = item.get('record_id')

            to_create = []
            to_update = []
            
            for order in orders:
                fields = {}
                for m in current_mapping:
                    xiaoe_path = m['xiaoe']
                    
                    def extract_value(obj, path_parts):
                        if not path_parts:
                            return obj
                        
                        part = path_parts[0]
                        remaining = path_parts[1:]
                        
                        if not part:
                            return extract_value(obj, remaining)
                        
                        if part == '[]':
                            if isinstance(obj, list):
                                if not remaining:
                                    return obj
                                results = []
                                for item in obj:
                                    result = extract_value(item, remaining)
                                    if isinstance(result, list):
                                        results.extend(result)
                                    else:
                                        results.append(result)
                                return results if results else ''
                            else:
                                return ''
                        
                        elif isinstance(obj, dict):
                            val = obj.get(part, '')
                            return extract_value(val, remaining)
                        
                        elif isinstance(obj, list):
                            results = []
                            for item in obj:
                                if isinstance(item, dict):
                                    val = item.get(part, '')
                                    result = extract_value(val, remaining)
                                    if isinstance(result, list):
                                        results.extend(result)
                                    else:
                                        results.append(result)
                            return results if results else ''
                        
                        else:
                            return ''
                    
                    parts = xiaoe_path.replace('[]', '.[]').split('.')
                    val = extract_value(order, parts)
                    
                    if isinstance(val, list):
                        val = json.dumps(val, ensure_ascii=False)
                    elif val is None:
                        val = ""
                    else:
                        val = str(val)
                    
                    fields[m['feishu']] = val
                
                order_unique_val = str(fields.get(unique_feishu_col, ''))
                
                if order_unique_val in feishu_id_map:
                    to_update.append({
                        "record_id": feishu_id_map[order_unique_val],
                        "fields": fields
                    })
                else:
                    to_create.append({"fields": fields})

            success_count = 0
            if to_create:
                if feishu.batch_create_records(config['feishu_app_token'], config['feishu_table_id'], to_create):
                    self.log(f"成功新建 {len(to_create)} 条记录")
                    success_count += len(to_create)
                else:
                    self.log("新建记录失败")
            
            if to_update:
                if feishu.batch_update_records(config['feishu_app_token'], config['feishu_table_id'], to_update):
                    self.log(f"成功更新 {len(to_update)} 条记录")
                    success_count += len(to_update)
                else:
                    self.log("更新记录失败")

            if success_count > 0:
                self.log(f"对比同步完成! 总计处理 {success_count} 条数据")
            else:
                self.log("无新数据需要处理")
        except Exception as e:
            self.log(f"同步异常: {str(e)}")

    def toggle_sync(self):
        if not self.is_syncing:
            self.is_syncing = True
            self.sync_btn.config(text="停止自动同步")
            self.log("自动同步已开启")
            self.run_scheduler()
        else:
            self.is_syncing = False
            self.sync_btn.config(text="开始自动同步")
            self.log("自动同步已停止")
            schedule.clear()

    def run_scheduler(self):
        interval = int(self.inputs['sync_interval'].get() or 60)
        schedule.every(interval).minutes.do(self.sync_once)
        def _loop():
            while self.is_syncing:
                schedule.run_pending()
                time.sleep(1)
        threading.Thread(target=_loop, daemon=True).start()

if __name__ == "__main__":
    root = tk.Tk()
    app = SyncApp(root)
    root.mainloop()
