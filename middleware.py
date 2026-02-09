from django.shortcuts import redirect
from django.urls import reverse

class ForcePasswordChangeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.user.is_authenticated:
            try:
                profile = request.user.profile
                if profile.force_password_change:
                    allowed_paths = [
                        reverse('password_change_forced'),
                        reverse('logout'),
                    ]
                    
                    
                    if request.path not in allowed_paths and not request.path.startswith('/static/') and not request.path.startswith('/admin/'):
                        return redirect('password_change_forced')
            except Exception:
                pass

        response = self.get_response(request)
        return response