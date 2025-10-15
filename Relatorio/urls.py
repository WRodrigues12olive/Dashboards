from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('sla-violado/<str:grupo_sla>/', views.sla_violado_view, name='sla_violado_lista'),
    path('extrair/', views.extracao_view, name='extracao_relatorios'),
    path('extrair/resultado/', views.resultado_view, name='resultado_relatorio'),
    path('extrair/download-excel/', views.gerar_excel_view, name='download_excel'),
    path('api/get-categorias/', views.api_get_categorias_view, name='api_get_categorias'),
]