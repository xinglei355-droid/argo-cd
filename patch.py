import re

with open('server/application/application.go', 'r') as f:
    content = f.read()

content = content.replace(
'''func (s *Server) getAppEnforceRBAC(ctx context.Context, action, project, namespace, name string, getApp func() (*v1alpha1.Application, error)) (*v1alpha1.Application, *v1alpha1.AppProject, error) {
	user := session.Username(ctx)
	if user == "" {
		user = "Unknown user"
	}
	logCtx := log.WithFields(map[string]any{
		"user":        user,
		"application": name,
		"namespace":   namespace,
	})
	if project != "" {
		// The user has provided everything we need to perform an initial RBAC check.
		givenRBACName := security.RBACName(s.ns, project, namespace, name)
		if err := s.enf.EnforceErr(ctx.Value("claims"), rbac.ResourceApplications, action, givenRBACName); err != nil {
			logCtx.WithFields(map[string]any{
				"project":                project,
				argocommon.SecurityField: argocommon.SecurityMedium,
			}).Warnf("user tried to %s application which they do not have access to: %s", action, err)''',
'''// applicationQueryContext contains the context for querying an application
type applicationQueryContext struct {
	name          string
	appNamespace  string
	project       string
	rbacSubject   any
	requestSource string
}

func (s *Server) constructApplicationQueryContext(ctx context.Context, name, appNamespace, project string, projects []string) (*applicationQueryContext, error) {
	appNs := s.appNamespaceOrDefault(appNamespace)

	proj := project
	if proj == "" && len(projects) > 0 {
		if len(projects) == 1 {
			proj = projects[0]
		} else {
			return nil, status.Errorf(codes.InvalidArgument, "multiple projects specified - the get endpoint accepts either zero or one project")
		}
	}

	user := session.Username(ctx)
	if user == "" {
		user = "Unknown user"
	}

	return &applicationQueryContext{
		name:          name,
		appNamespace:  appNs,
		project:       proj,
		rbacSubject:   ctx.Value("claims"),
		requestSource: user,
	}, nil
}

func (s *Server) getAppEnforceRBAC(ctx context.Context, action string, qc *applicationQueryContext, getApp func() (*v1alpha1.Application, error)) (*v1alpha1.Application, *v1alpha1.AppProject, error) {
	logCtx := log.WithFields(map[string]any{
		"user":        qc.requestSource,
		"application": qc.name,
		"namespace":   qc.appNamespace,
	})
	if qc.project != "" {
		// The user has provided everything we need to perform an initial RBAC check.
		givenRBACName := security.RBACName(s.ns, qc.project, qc.appNamespace, qc.name)
		if err := s.enf.EnforceErr(qc.rbacSubject, rbac.ResourceApplications, action, givenRBACName); err != nil {
			logCtx.WithFields(map[string]any{
				"project":                qc.project,
				argocommon.SecurityField: argocommon.SecurityMedium,
			}).Warnf("user tried to %s application which they do not have access to: %s", action, err)'''
)

content = content.replace(
'''			if project != "" {
				// We know that the user was allowed to get the Application, but the Application does not exist. Return 404.
				return nil, nil, status.Error(codes.NotFound, apierrors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "applications"}, name).Error())
			}''',
'''			if qc.project != "" {
				// We know that the user was allowed to get the Application, but the Application does not exist. Return 404.
				return nil, nil, status.Error(codes.NotFound, apierrors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "applications"}, qc.name).Error())
			}'''
)

content = content.replace(
'''	if err := s.enf.EnforceErr(ctx.Value("claims"), rbac.ResourceApplications, action, a.RBACName(s.ns)); err != nil {
		logCtx.WithFields(map[string]any{
			"project":                a.Spec.Project,
			argocommon.SecurityField: argocommon.SecurityMedium,
		}).Warnf("user tried to %s application which they do not have access to: %s", action, err)
		if project != "" {
			// The user specified a project. We would have returned a 404 if the user had access to the app, but the app
			// did not exist. So we have to return a 404 when the app does exist, but the user does not have access.
			// Otherwise, they could infer that the app exists based on the error code.
			return nil, nil, status.Error(codes.NotFound, apierrors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "applications"}, name).Error())
		}''',
'''	if err := s.enf.EnforceErr(qc.rbacSubject, rbac.ResourceApplications, action, a.RBACName(s.ns)); err != nil {
		logCtx.WithFields(map[string]any{
			"project":                a.Spec.Project,
			argocommon.SecurityField: argocommon.SecurityMedium,
		}).Warnf("user tried to %s application which they do not have access to: %s", action, err)
		if qc.project != "" {
			// The user specified a project. We would have returned a 404 if the user had access to the app, but the app
			// did not exist. So we have to return a 404 when the app does exist, but the user does not have access.
			// Otherwise, they could infer that the app exists based on the error code.
			return nil, nil, status.Error(codes.NotFound, apierrors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "applications"}, qc.name).Error())
		}'''
)

content = content.replace(
'''	if project != "" && effectiveProject != project {
		logCtx.WithFields(map[string]any{
			"project":                a.Spec.Project,
			argocommon.SecurityField: argocommon.SecurityMedium,
		}).Warnf("user tried to %s application in project %s, but the application is in project %s", action, project, effectiveProject)
		// The user has access to the app, but the app is in a different project. Return 404, meaning "app doesn't
		// exist in that project".
		return nil, nil, status.Error(codes.NotFound, apierrors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "applications"}, name).Error())
	}''',
'''	if qc.project != "" && effectiveProject != qc.project {
		logCtx.WithFields(map[string]any{
			"project":                a.Spec.Project,
			argocommon.SecurityField: argocommon.SecurityMedium,
		}).Warnf("user tried to %s application in project %s, but the application is in project %s", action, qc.project, effectiveProject)
		// The user has access to the app, but the app is in a different project. Return 404, meaning "app doesn't
		// exist in that project".
		return nil, nil, status.Error(codes.NotFound, apierrors.NewNotFound(schema.GroupResource{Group: "argoproj.io", Resource: "applications"}, qc.name).Error())
	}'''
)

content = content.replace(
'''func (s *Server) getApplicationEnforceRBACInformer(ctx context.Context, action, project, namespace, name string) (*v1alpha1.Application, *v1alpha1.AppProject, error) {
	namespaceOrDefault := s.appNamespaceOrDefault(namespace)
	return s.getAppEnforceRBAC(ctx, action, project, namespaceOrDefault, name, func() (*v1alpha1.Application, error) {
		if !s.isNamespaceEnabled(namespaceOrDefault) {
			return nil, security.NamespaceNotPermittedError(namespaceOrDefault)
		}
		return s.appLister.Applications(namespaceOrDefault).Get(name)
	})
}''',
'''func (s *Server) getApplicationEnforceRBACInformer(ctx context.Context, action string, qc *applicationQueryContext) (*v1alpha1.Application, *v1alpha1.AppProject, error) {
	return s.getAppEnforceRBAC(ctx, action, qc, func() (*v1alpha1.Application, error) {
		if !s.isNamespaceEnabled(qc.appNamespace) {
			return nil, security.NamespaceNotPermittedError(qc.appNamespace)
		}
		return s.appLister.Applications(qc.appNamespace).Get(qc.name)
	})
}'''
)

content = content.replace(
'''func (s *Server) getApplicationEnforceRBACClient(ctx context.Context, action, project, namespace, name, resourceVersion string) (*v1alpha1.Application, *v1alpha1.AppProject, error) {
	namespaceOrDefault := s.appNamespaceOrDefault(namespace)
	return s.getAppEnforceRBAC(ctx, action, project, namespaceOrDefault, name, func() (*v1alpha1.Application, error) {
		if !s.isNamespaceEnabled(namespaceOrDefault) {
			return nil, security.NamespaceNotPermittedError(namespaceOrDefault)
		}
		app, err := s.appclientset.ArgoprojV1alpha1().Applications(namespaceOrDefault).Get(ctx, name, metav1.GetOptions{
			ResourceVersion: resourceVersion,
		})
		if err != nil {
			return nil, err
		}
		return app, nil
	})
}''',
'''func (s *Server) getApplicationEnforceRBACClient(ctx context.Context, action string, qc *applicationQueryContext, resourceVersion string) (*v1alpha1.Application, *v1alpha1.AppProject, error) {
	return s.getAppEnforceRBAC(ctx, action, qc, func() (*v1alpha1.Application, error) {
		if !s.isNamespaceEnabled(qc.appNamespace) {
			return nil, security.NamespaceNotPermittedError(qc.appNamespace)
		}
		app, err := s.appclientset.ArgoprojV1alpha1().Applications(qc.appNamespace).Get(ctx, qc.name, metav1.GetOptions{
			ResourceVersion: resourceVersion,
		})
		if err != nil {
			return nil, err
		}
		return app, nil
	})
}'''
)

# Apply changes for `Get`, `Delete`, `Sync`, `UpdateSpec`, `Patch`, `ListResourceEvents`, `validateAndUpdateApp`, `GetResource`, `PatchResource`, `DeleteResource`, `PodLogs`, `Rollback`, `TerminateOperation`, `ListLinks`, `getUnstructuredLiveResourceOrApp`

content = re.sub(
r'''func \(s \*Server\) Get\(ctx context\.Context, q \*application\.ApplicationQuery\) \(\*v1alpha1\.Application, error\) \{
	appName := q\.GetName\(\)
	appNs := s\.appNamespaceOrDefault\(q\.GetAppNamespace\(\)\)

	project := ""
	projects := getProjectsFromApplicationQuery\(\*q\)
	if len\(projects\) == 1 \{
		project = projects\[0\]
	\} else if len\(projects\) > 1 \{
		return nil, status\.Errorf\(codes\.InvalidArgument, "multiple projects specified - the get endpoint accepts either zero or one project"\)
	\}

	// We must use a client Get instead of an informer Get, because it's common to call Get immediately
	// following a Watch \(which is not yet powered by an informer\), and the Get must reflect what was
	// previously seen by the client\.
	a, proj, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionGet, project, appNs, appName, q\.GetResourceVersion\(\)\)''',
r'''func (s *Server) Get(ctx context.Context, q *application.ApplicationQuery) (*v1alpha1.Application, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), "", getProjectsFromApplicationQuery(*q))
	if err != nil {
		return nil, err
	}

	// We must use a client Get instead of an informer Get, because it's common to call Get immediately
	// following a Watch (which is not yet powered by an informer), and the Get must reflect what was
	// previously seen by the client.
	a, proj, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionGet, qc, q.GetResourceVersion())''',
content)

content = re.sub(
r'''	events := make\(chan \*v1alpha1\.ApplicationWatchEvent, watchAPIBufferSize\)
	unsubscribe := s\.appBroadcaster\.Subscribe\(events, func\(event \*v1alpha1\.ApplicationWatchEvent\) bool \{
		return event\.Application\.Name == appName && event\.Application\.Namespace == appNs
	\}\)''',
r'''	events := make(chan *v1alpha1.ApplicationWatchEvent, watchAPIBufferSize)
	unsubscribe := s.appBroadcaster.Subscribe(events, func(event *v1alpha1.ApplicationWatchEvent) bool {
		return event.Application.Name == qc.name && event.Application.Namespace == qc.appNamespace
	})''',
content)

content = re.sub(
r'''	app, err := argo\.RefreshApp\(appIf, appName, refreshType, hydrateType\)''',
r'''	app, err := argo.RefreshApp(appIf, qc.name, refreshType, hydrateType)''',
content)

content = re.sub(
r'''				AppName:            appName,''',
r'''				AppName:            qc.name,''',
content)


# ListResourceEvents
content = re.sub(
r'''func \(s \*Server\) ListResourceEvents\(ctx context\.Context, q \*application\.ApplicationResourceEventsQuery\) \(\*eventspb\.EventList, error\) \{
	a, p, err := s\.getApplicationEnforceRBACInformer\(ctx, rbac\.ActionGet, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\)\)''',
r'''func (s *Server) ListResourceEvents(ctx context.Context, q *application.ApplicationResourceEventsQuery) (*eventspb.EventList, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, p, err := s.getApplicationEnforceRBACInformer(ctx, rbac.ActionGet, qc)''',
content)

# validateAndUpdateApp
content = re.sub(
r'''func \(s \*Server\) validateAndUpdateApp\(ctx context\.Context, newApp \*v1alpha1\.Application, merge bool, validate bool, action string, currentProject string\) \(\*v1alpha1\.Application, error\) \{
	s\.projectLock\.RLock\(newApp\.Spec\.GetProject\(\)\)
	defer s\.projectLock\.RUnlock\(newApp\.Spec\.GetProject\(\)\)

	app, proj, err := s\.getApplicationEnforceRBACClient\(ctx, action, currentProject, newApp\.Namespace, newApp\.Name, ""\)''',
r'''func (s *Server) validateAndUpdateApp(ctx context.Context, newApp *v1alpha1.Application, merge bool, validate bool, action string, currentProject string) (*v1alpha1.Application, error) {
	s.projectLock.RLock(newApp.Spec.GetProject())
	defer s.projectLock.RUnlock(newApp.Spec.GetProject())

	qc, err := s.constructApplicationQueryContext(ctx, newApp.Name, newApp.Namespace, currentProject, nil)
	if err != nil {
		return nil, err
	}
	app, proj, err := s.getApplicationEnforceRBACClient(ctx, action, qc, "")''',
content)

# UpdateSpec
content = re.sub(
r'''func \(s \*Server\) UpdateSpec\(ctx context\.Context, q \*application\.ApplicationUpdateSpecRequest\) \(\*v1alpha1\.ApplicationSpec, error\) \{
	if q\.GetSpec\(\) == nil \{
		return nil, errors\.New\("error updating application spec: spec is nil in request"\)
	\}
	a, _, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionUpdate, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\), ""\)''',
r'''func (s *Server) UpdateSpec(ctx context.Context, q *application.ApplicationUpdateSpecRequest) (*v1alpha1.ApplicationSpec, error) {
	if q.GetSpec() == nil {
		return nil, errors.New("error updating application spec: spec is nil in request")
	}
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, _, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionUpdate, qc, "")''',
content)

# Patch
content = re.sub(
r'''func \(s \*Server\) Patch\(ctx context\.Context, q \*application\.ApplicationPatchRequest\) \(\*v1alpha1\.Application, error\) \{
	app, _, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionGet, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\), ""\)''',
r'''func (s *Server) Patch(ctx context.Context, q *application.ApplicationPatchRequest) (*v1alpha1.Application, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	app, _, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionGet, qc, "")''',
content)

# Delete
content = re.sub(
r'''func \(s \*Server\) Delete\(ctx context\.Context, q \*application\.ApplicationDeleteRequest\) \(\*application\.ApplicationResponse, error\) \{
	appName := q\.GetName\(\)
	appNs := s\.appNamespaceOrDefault\(q\.GetAppNamespace\(\)\)
	a, _, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionGet, q\.GetProject\(\), appNs, appName, ""\)''',
r'''func (s *Server) Delete(ctx context.Context, q *application.ApplicationDeleteRequest) (*application.ApplicationResponse, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, _, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionGet, qc, "")''',
content)

content = re.sub(
r'''	err = s\.appclientset\.ArgoprojV1alpha1\(\)\.Applications\(appNs\)\.Delete\(ctx, appName, metav1\.DeleteOptions\{\}\)''',
r'''	err = s.appclientset.ArgoprojV1alpha1().Applications(qc.appNamespace).Delete(ctx, qc.name, metav1.DeleteOptions{})''',
content)

# Sync
content = re.sub(
r'''func \(s \*Server\) Sync\(ctx context\.Context, syncReq \*application\.ApplicationSyncRequest\) \(\*v1alpha1\.Application, error\) \{
	a, proj, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionGet, syncReq\.GetProject\(\), syncReq\.GetAppNamespace\(\), syncReq\.GetName\(\), ""\)''',
r'''func (s *Server) Sync(ctx context.Context, syncReq *application.ApplicationSyncRequest) (*v1alpha1.Application, error) {
	qc, err := s.constructApplicationQueryContext(ctx, syncReq.GetName(), syncReq.GetAppNamespace(), syncReq.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, proj, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionGet, qc, "")''',
content)

content = re.sub(
r'''	appName := syncReq\.GetName\(\)
	appNs := s\.appNamespaceOrDefault\(syncReq\.GetAppNamespace\(\)\)
	appIf := s\.appclientset\.ArgoprojV1alpha1\(\)\.Applications\(appNs\)
	a, err = argo\.SetAppOperation\(appIf, appName, &op\)''',
r'''	appIf := s.appclientset.ArgoprojV1alpha1().Applications(qc.appNamespace)
	a, err = argo.SetAppOperation(appIf, qc.name, &op)''',
content)

# Rollback
content = re.sub(
r'''func \(s \*Server\) Rollback\(ctx context\.Context, rollbackReq \*application\.ApplicationRollbackRequest\) \(\*v1alpha1\.Application, error\) \{
	a, _, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionSync, rollbackReq\.GetProject\(\), rollbackReq\.GetAppNamespace\(\), rollbackReq\.GetName\(\), ""\)''',
r'''func (s *Server) Rollback(ctx context.Context, rollbackReq *application.ApplicationRollbackRequest) (*v1alpha1.Application, error) {
	qc, err := s.constructApplicationQueryContext(ctx, rollbackReq.GetName(), rollbackReq.GetAppNamespace(), rollbackReq.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, _, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionSync, qc, "")''',
content)

content = re.sub(
r'''	appName := rollbackReq\.GetName\(\)
	appNs := s\.appNamespaceOrDefault\(rollbackReq\.GetAppNamespace\(\)\)
	appIf := s\.appclientset\.ArgoprojV1alpha1\(\)\.Applications\(appNs\)
	a, err = argo\.SetAppOperation\(appIf, appName, &op\)''',
r'''	appIf := s.appclientset.ArgoprojV1alpha1().Applications(qc.appNamespace)
	a, err = argo.SetAppOperation(appIf, qc.name, &op)''',
content)

# TerminateOperation
content = re.sub(
r'''func \(s \*Server\) TerminateOperation\(ctx context\.Context, termOpReq \*application\.OperationTerminateRequest\) \(\*application\.OperationTerminateResponse, error\) \{
	a, _, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionSync, termOpReq\.GetProject\(\), termOpReq\.GetAppNamespace\(\), termOpReq\.GetName\(\), ""\)''',
r'''func (s *Server) TerminateOperation(ctx context.Context, termOpReq *application.OperationTerminateRequest) (*application.OperationTerminateResponse, error) {
	qc, err := s.constructApplicationQueryContext(ctx, termOpReq.GetName(), termOpReq.GetAppNamespace(), termOpReq.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, _, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionSync, qc, "")''',
content)

# ListLinks
content = re.sub(
r'''func \(s \*Server\) ListLinks\(ctx context\.Context, req \*application\.ApplicationResourceRequest\) \(\*application\.LinksResponse, error\) \{
	a, proj, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionGet, req\.GetProject\(\), req\.GetAppNamespace\(\), req\.GetName\(\), ""\)''',
r'''func (s *Server) ListLinks(ctx context.Context, req *application.ApplicationResourceRequest) (*application.LinksResponse, error) {
	qc, err := s.constructApplicationQueryContext(ctx, req.GetName(), req.GetAppNamespace(), req.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, proj, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionGet, qc, "")''',
content)

# getUnstructuredLiveResourceOrApp
content = re.sub(
r'''func \(s \*Server\) getUnstructuredLiveResourceOrApp\(ctx context\.Context, rbacRequest string, q \*application\.ApplicationResourceRequest\) \(obj \*unstructured\.Unstructured, res \*v1alpha1\.ResourceNode, app \*v1alpha1\.Application, config \*rest\.Config, err error\) \{
	app, p, err := s\.getApplicationEnforceRBACInformer\(ctx, rbacRequest, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\)\)''',
r'''func (s *Server) getUnstructuredLiveResourceOrApp(ctx context.Context, rbacRequest string, q *application.ApplicationResourceRequest) (obj *unstructured.Unstructured, res *v1alpha1.ResourceNode, app *v1alpha1.Application, config *rest.Config, err error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	app, p, err := s.getApplicationEnforceRBACInformer(ctx, rbacRequest, qc)''',
content)

# GetResource
content = re.sub(
r'''func \(s \*Server\) GetResource\(ctx context\.Context, q \*application\.ApplicationResourceRequest\) \(\*application\.ApplicationResourceResponse, error\) \{
	a, proj, err := s\.getApplicationEnforceRBACClient\(ctx, rbac\.ActionGet, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\), ""\)''',
r'''func (s *Server) GetResource(ctx context.Context, q *application.ApplicationResourceRequest) (*application.ApplicationResourceResponse, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, proj, err := s.getApplicationEnforceRBACClient(ctx, rbac.ActionGet, qc, "")''',
content)

# PatchResource
content = re.sub(
r'''func \(s \*Server\) PatchResource\(ctx context\.Context, q \*application\.ApplicationResourcePatchRequest\) \(\*application\.ApplicationResponse, error\) \{
	a, p, err := s\.getApplicationEnforceRBACInformer\(ctx, rbac\.ActionGet, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\)\)''',
r'''func (s *Server) PatchResource(ctx context.Context, q *application.ApplicationResourcePatchRequest) (*application.ApplicationResponse, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, p, err := s.getApplicationEnforceRBACInformer(ctx, rbac.ActionGet, qc)''',
content)

# DeleteResource
content = re.sub(
r'''func \(s \*Server\) DeleteResource\(ctx context\.Context, q \*application\.ApplicationResourceDeleteRequest\) \(\*application\.ApplicationResponse, error\) \{
	a, p, err := s\.getApplicationEnforceRBACInformer\(ctx, rbac\.ActionGet, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\)\)''',
r'''func (s *Server) DeleteResource(ctx context.Context, q *application.ApplicationResourceDeleteRequest) (*application.ApplicationResponse, error) {
	qc, err := s.constructApplicationQueryContext(ctx, q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return nil, err
	}
	a, p, err := s.getApplicationEnforceRBACInformer(ctx, rbac.ActionGet, qc)''',
content)

# PodLogs
content = re.sub(
r'''func \(s \*Server\) PodLogs\(q \*application\.ApplicationPodLogsQuery, ws application\.ApplicationService_PodLogsServer\) error \{
	a, p, err := s\.getApplicationEnforceRBACInformer\(ws\.Context\(\), rbac\.ActionGet, q\.GetProject\(\), q\.GetAppNamespace\(\), q\.GetName\(\)\)''',
r'''func (s *Server) PodLogs(q *application.ApplicationPodLogsQuery, ws application.ApplicationService_PodLogsServer) error {
	qc, err := s.constructApplicationQueryContext(ws.Context(), q.GetName(), q.GetAppNamespace(), q.GetProject(), nil)
	if err != nil {
		return err
	}
	a, p, err := s.getApplicationEnforceRBACInformer(ws.Context(), rbac.ActionGet, qc)''',
content)


with open('server/application/application.go', 'w') as f:
    f.write(content)
