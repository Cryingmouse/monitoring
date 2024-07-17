import logging

from monitoring.tasks.base import AbstractTask

LOG = logging.getLogger()
#
#
# def k3s_service():
#     cmd = "systemctl status k3s"
#     ret, msg = subprocess.getstatusoutput(cmd)
#     if not ret:
#         state = re.compile(r"Active: (.*)(?P<state>[(]([a-z]*)[)]) since")
#         re_result = state.search(msg)
#         status = re_result.group(1).strip() if re_result else None
#         if status == "active":
#             return True
#     return False
#
#
# def get_date():
#     cmd = (
#         "openssl s_client -connect localhost:6443 -showcerts </dev/null 2>&1 | openssl x509 -noout -startdate -enddate"
#     )
#     ret, msg = subprocess.getstatusoutput(cmd)
#     if ret:
#         date = None
#     else:
#         dates = data.findall(msg)
#         due_date = dates[1]
#         present_data = time.asctime(time.localtime(time.time()))[4:]
#         present_data = parse(present_data)
#         due_date = parse(due_date)
#         date = (due_date - present_data).days
#     return date
#
#
# def rotate_certificate():
#     state = k3s_service()
#     if state:
#         deviation = get_date()
#         if deviation and deviation < 90:
#             # 删掉secretk3s-serving
#             cmd = "kubectl --insecure-skip-tls-verify -n kube-system delete secrets k3s-serving"
#             subprocess.getstatusoutput(cmd)
#             # 删掉系统中的文件dynamic-cert.json
#             cmd = "rm -f /var/lib/rancher/k3s/server/tls/dynamic-cert.json"
#             subprocess.getstatusoutput(cmd)
#             # 重启k3s进程
#             cmd = "systemctl restart k3s"
#             subprocess.getstatusoutput(cmd)


class RotateK3SCertificateTask(AbstractTask):
    @classmethod
    def execute(cls, *args, **kwargs):
        LOG.info(f"Start to check if it is necessary to rotate K3s certificate.")
        # rotate_certificate()
